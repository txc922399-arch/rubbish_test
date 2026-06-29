"""
SEC 年报下载工具 (10-K / 20-F / N-CSR)
高稳定性版本 —— 支持断点续传、重试、完整历史数据、日志记录

功能:
- 从CSV读取ticker列表，自动映射到CIK
- 下载2000年后的 10-K（美资公司）、20-F（外国公司）、N-CSR（基金/注册投资公司）年报
- 自动选择每年最佳版本（优先 10-K > 20-F > N-CSR，同类型优先非 /A 修正版）
- 内置重试机制、速率控制、断点续传
- 找不到目标年报时安全跳过
"""

import requests
import os
import csv
import time
import re
import json
import logging
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from tqdm import tqdm

# ============================================================
# ====================== 配置区域 ============================
# ============================================================

# SEC官方要求提供身份标识
# 格式必须符合 SEC 白名单正则：`Project/Version (email)`
# SEC 网关会对不匹配此格式的 UA 直接拦截（返回 503/403）
USER_AGENT = "PersonalResearchProject/2.0 (txc922399@gmail.com)"

# CSV文件路径（包含ticker列的csv文件）
CSV_PATH = r"D:\last_demo\tickers_10.csv"

# 年报保存根目录
SAVE_ROOT = r"D:\last_demo\tickers_html"

# 日志文件路径（None表示使用默认路径 SAVE_ROOT/download.log）
LOG_PATH = None

# 断点续传记录文件路径（None表示使用默认路径）
PROGRESS_PATH = None

# 并发下载线程数
# SEC 实际限制: 每秒 10 个请求 (每个 IP)
# 3 线程 × 0.15s 间隔 ≈ 22 req/s 峰值, 有限流保护
# 自适应机制会在连续 503 时自动降速, 所以这里可以激进一些
MAX_WORKERS = 3

# 起始年份 (用户要求 2000 年后)
START_YEAR = 2000

# 请求超时 (秒)
REQUEST_TIMEOUT = 60

# 最大重试次数
MAX_RETRIES = 3

# 基础退避时间 (秒)
BASE_BACKOFF = 2

# 请求间隔 (秒), 遵守 SEC 速率限制 (每秒不超过 10 次)
# 配合 3 线程 ≈ 22 req/s 峰值, 触发限流后自适应降速
REQUEST_DELAY = 0.15

# 最小文件大小 (字节), 小于此值视为无效
MIN_FILE_SIZE = 5000

# 全局速率自适应: 连续 503 后自动降低请求频率
# 连续 N 次 503 后, 每个请求额外等待 PENALTY_SLEEP 秒
# 自适应会临时把 REQUEST_DELAY 翻倍, 成功若干次后再恢复
RATE_LIMIT_PENALTY_THRESHOLD = 3
RATE_LIMIT_PENALTY_SLEEP = 3.0

# 本地缓存目录 (存放 CIK{cik}.json 和历史分片, 减少重复请求)
CACHE_DIR_NAME = "sec_cache"

# 是否启用本地缓存 (True=启用, 减少网络请求和报错)
USE_CACHE = True

# ============================================================
# ====================== 配置结束 ============================
# ============================================================

# SEC API 请求头
# 注意：不要手动设置 Host 头！requests 会自动管理；手动设置会被识别为伪造请求
# Accept 头声明只接受 HTML，避免被推送非预期格式
HEADERS_DATA = {
    "User-Agent": USER_AGENT,
    "Accept-Encoding": "gzip, deflate",
    "Accept": "application/json, text/plain, */*",
    "Connection": "keep-alive",
}
HEADERS_EDGAR = {
    "User-Agent": USER_AGENT,
    "Accept-Encoding": "gzip, deflate",
    "Accept": "text/html, application/xhtml+xml, application/xml;q=0.9, */*;q=0.8",
    "Connection": "keep-alive",
}
HEADERS_WWW = {
    "User-Agent": USER_AGENT,
    "Accept-Encoding": "gzip, deflate",
    "Accept": "application/json, text/plain, */*",
    "Connection": "keep-alive",
}

# 线程安全的打印锁
_print_lock = Lock()

# 全局 Session，复用 TCP 连接（HTTP keep-alive）
# 比每次新建连接更稳定，握手少
_session = None
_session_lock = Lock()

# ============================================================
#           全局速率自适应（连续 503 自动降速）
# ============================================================
# 思路：维护一个连续失败计数器，超过阈值就把请求间隔临时翻倍
# 成功若干次后逐渐恢复
_rate_state_lock = Lock()
_consecutive_rate_limits = 0  # 连续 503/429 次数
_consecutive_successes = 0    # 连续成功次数
_current_delay_multiplier = 1.0  # 当前间隔倍数（1.0=正常, 2.0=翻倍）


def get_session() -> requests.Session:
    """获取全局 Session（线程安全单例）"""
    global _session
    if _session is None:
        with _session_lock:
            if _session is None:
                _session = requests.Session()
                # 设置默认连接池大小
                adapter = requests.adapters.HTTPAdapter(
                    pool_connections=4,
                    pool_maxsize=8,
                    max_retries=0  # 我们自己实现重试
                )
                _session.mount("https://", adapter)
                _session.mount("http://", adapter)
    return _session


def _record_rate_limit_hit():
    """记录一次限流事件, 触发自适应降速"""
    global _consecutive_rate_limits, _consecutive_successes, _current_delay_multiplier
    with _rate_state_lock:
        _consecutive_rate_limits += 1
        _consecutive_successes = 0
        if _consecutive_rate_limits >= RATE_LIMIT_PENALTY_THRESHOLD:
            # 翻倍间隔, 最多 4 倍 (即 0.15s × 4 = 0.6s)
            _current_delay_multiplier = min(_current_delay_multiplier * 2.0, 4.0)


def _record_success():
    """记录一次成功请求, 连续成功后逐渐恢复正常间隔"""
    global _consecutive_rate_limits, _consecutive_successes, _current_delay_multiplier
    with _rate_state_lock:
        _consecutive_rate_limits = 0
        _consecutive_successes += 1
        # 每 20 次成功就把倍数减半, 直到 1.0
        if _consecutive_successes >= 20 and _current_delay_multiplier > 1.0:
            _current_delay_multiplier = max(_current_delay_multiplier / 2.0, 1.0)
            _consecutive_successes = 0


def get_effective_delay() -> float:
    """获取当前生效的请求间隔 (含自适应倍数)"""
    with _rate_state_lock:
        return REQUEST_DELAY * _current_delay_multiplier


def rate_limited_sleep():
    """自适应 sleep: 等待当前生效的间隔, 并在限流时额外等待"""
    delay = get_effective_delay()
    with _rate_state_lock:
        penalty = RATE_LIMIT_PENALTY_SLEEP if _consecutive_rate_limits >= RATE_LIMIT_PENALTY_THRESHOLD else 0.0
    time.sleep(delay + penalty)

# 目标表单类型：10-K（美资公司年报）、20-F（外国公司年报）、N-CSR（基金/注册投资公司年报）
TARGET_FORMS = {"10-K", "20-F", "10-K/A", "20-F/A", "N-CSR", "N-CSR/A"}

# 表单优先级
# 1) 10-K 优先于 20-F 优先于 N-CSR（公司年报 > 外国公司年报 > 基金年报）
# 2) 同一类型下优先非 /A 修正版
FORM_PRIORITY = ["10-K", "10-K/A", "20-F", "20-F/A", "N-CSR", "N-CSR/A"]


# ============================================================
#                       工具函数
# ============================================================


def safe_print(*args, **kwargs):
    """线程安全的打印"""
    with _print_lock:
        tqdm.write(" ".join(str(a) for a in args), **kwargs)


def sanitize_filename(name: str) -> str:
    """清理文件名中的非法字符（Windows兼容）"""
    # Windows非法字符: \ / : * ? " < > |
    illegal_chars = r'\/:*?"<>|'
    for ch in illegal_chars:
        name = name.replace(ch, "_")
    # 去掉前后空格和点
    name = name.strip(". ")
    # 限制长度
    if len(name) > 200:
        name = name[:200]
    return name


def setup_logging(log_path: str = None) -> logging.Logger:
    """配置日志系统"""
    if log_path is None:
        log_path = os.path.join(SAVE_ROOT, "download.log")

    os.makedirs(os.path.dirname(log_path), exist_ok=True)

    logger = logging.getLogger("SEC_Downloader")
    logger.setLevel(logging.INFO)

    # 清除已有的handler（防止重复）
    logger.handlers.clear()

    # 文件handler
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(logging.INFO)
    fh.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    ))
    logger.addHandler(fh)

    # 控制台handler（只输出WARNING以上）
    ch = logging.StreamHandler()
    ch.setLevel(logging.WARNING)
    ch.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
    logger.addHandler(ch)

    return logger


def load_progress(progress_path: str) -> dict:
    """加载断点续传记录"""
    if progress_path is None:
        progress_path = os.path.join(SAVE_ROOT, "progress.json")
    if os.path.exists(progress_path):
        try:
            with open(progress_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            pass
    return {}


def save_progress(progress: dict, progress_path: str = None):
    """保存断点续传记录"""
    if progress_path is None:
        progress_path = os.path.join(SAVE_ROOT, "progress.json")
    os.makedirs(os.path.dirname(progress_path), exist_ok=True)
    with open(progress_path, "w", encoding="utf-8") as f:
        json.dump(progress, f, indent=2, ensure_ascii=False)


def _compute_backoff(attempt: int, base: float = BASE_BACKOFF, jitter: bool = True) -> float:
    """
    计算指数退避时间（带随机抖动，避免雪崩）
    attempt: 第几次重试 (0-based)
    """
    import random
    backoff = base * (2 ** attempt)
    # 加上 0~30% 随机抖动，防止多线程同时重试造成雪崩
    if jitter:
        backoff = backoff * (1 + random.random() * 0.3)
    return backoff


def retry_request(method: str, url: str, headers: dict,
                  timeout: int = REQUEST_TIMEOUT,
                  max_retries: int = MAX_RETRIES,
                  logger: logging.Logger = None) -> requests.Response:
    """
    带指数退避+随机抖动的HTTP请求

    处理策略:
    - 503/502/504: SEC 网关限流/临时不可用，指数退避重试（最长 60s）
    - 429: 严格遵守 Retry-After 头
    - 403: 第一次出现时立即返回（UA/请求被拒），不再重试
    - 连接错误/超时: 指数退避重试
    """
    last_exception = None
    session = get_session()

    for attempt in range(max_retries + 1):
        try:
            resp = session.request(method, url, headers=headers, timeout=timeout)

            # 成功
            if 200 <= resp.status_code < 400:
                _record_success()
                return resp

            # 429 Too Many Requests — 严格遵守 Retry-After
            if resp.status_code == 429:
                _record_rate_limit_hit()
                retry_after = resp.headers.get("Retry-After")
                if retry_after:
                    try:
                        wait = float(retry_after)
                    except ValueError:
                        wait = _compute_backoff(attempt)
                else:
                    wait = _compute_backoff(attempt)
                # 429 至少等 10s
                wait = max(wait, 10)
                if logger:
                    logger.warning(f"429 限流 {url}，等待 {wait:.1f}s (尝试 {attempt+1}/{max_retries+1})")
                time.sleep(wait)
                continue

            # 503 Service Unavailable — SEC 网关限流
            # 这是最常见的错误，502/504 同理
            if resp.status_code in (502, 503, 504):
                _record_rate_limit_hit()
                wait = _compute_backoff(attempt, base=BASE_BACKOFF * 2)
                # 503 至少等 5s
                wait = max(wait, 5)
                if logger:
                    logger.warning(
                        f"{resp.status_code} 网关限流 {url}，等待 {wait:.1f}s "
                        f"(尝试 {attempt+1}/{max_retries+1})"
                    )
                time.sleep(wait)
                continue

            # 403 Forbidden — 通常是 UA/Host 头被拦截，重试无意义，立即返回
            if resp.status_code == 403:
                if logger:
                    logger.error(f"403 被拒 {url}，可能是 UA 不合规或 IP 限流")
                return resp

            # 4xx 客户端错误（404 等）不重试
            if resp.status_code >= 400:
                return resp

            # 5xx 其他情况
            wait = _compute_backoff(attempt)
            if logger:
                logger.warning(
                    f"{resp.status_code} {url}，等待 {wait:.1f}s "
                    f"(尝试 {attempt+1}/{max_retries+1})"
                )
            time.sleep(wait)
            continue

        except (requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                requests.exceptions.ChunkedEncodingError) as e:
            last_exception = e
            if attempt < max_retries:
                wait = _compute_backoff(attempt)
                if logger:
                    logger.warning(
                        f"连接错误 {url}: {type(e).__name__}，等待 {wait:.1f}s "
                        f"(尝试 {attempt+1}/{max_retries+1})"
                    )
                time.sleep(wait)
            else:
                if logger:
                    logger.error(f"最终连接失败 {url}: {e}")
                raise

    if last_exception:
        raise last_exception
    raise RuntimeError(f"请求失败，已达最大重试次数: {url}")


# ============================================================
#                    SEC API 交互层
# ============================================================


def get_ticker_cik_mapping(logger: logging.Logger = None) -> dict:
    """从SEC获取ticker到CIK的映射表（带重试）"""
    url = "https://www.sec.gov/files/company_tickers.json"
    resp = retry_request("GET", url, HEADERS_WWW, logger=logger)
    resp.raise_for_status()
    data = resp.json()

    mapping = {}
    for item in data.values():
        ticker = item["ticker"].upper()
        cik = str(item["cik_str"]).zfill(10)
        mapping[ticker] = cik
    return mapping


def load_tickers_from_csv(csv_path: str) -> list:
    """
    从CSV文件加载ticker列表
    支持多种编码和列名
    """
    tickers = []

    # 尝试多种编码
    encodings = ["utf-8-sig", "utf-8", "gbk", "latin-1"]
    content = None
    used_encoding = None

    for enc in encodings:
        try:
            with open(csv_path, "r", encoding=enc) as f:
                content = f.read()
            used_encoding = enc
            break
        except (UnicodeDecodeError, UnicodeError):
            continue

    if content is None:
        raise ValueError(f"无法读取CSV文件 {csv_path}，尝试了编码: {encodings}")

    # 使用 csv.Sniffer 检测分隔符
    try:
        dialect = csv.Sniffer().sniff(content[:4096])
        delimiter = dialect.delimiter
    except csv.Error:
        delimiter = ","

    reader = csv.reader(content.splitlines(), delimiter=delimiter)
    header = next(reader, None)

    # 查找ticker列（不区分大小写）
    if header:
        ticker_col_idx = None
        for i, col in enumerate(header):
            col_clean = col.strip().lower()
            if col_clean in ("ticker", "symbol", "code", "tic"):
                ticker_col_idx = i
                break
        if ticker_col_idx is None:
            ticker_col_idx = 0  # 默认第一列

        for row in reader:
            if row and len(row) > ticker_col_idx:
                t = row[ticker_col_idx].strip().upper()
                if t and t not in tickers:  # 去重
                    tickers.append(t)

    return tickers


def _cache_path(cik: str, kind: str = "submissions", extra: str = "") -> str:
    """获取本地缓存文件路径"""
    cache_dir = os.path.join(SAVE_ROOT, CACHE_DIR_NAME)
    os.makedirs(cache_dir, exist_ok=True)
    if extra:
        # extra 形如 submissions_0001234_2014-10-31
        # 清洗成安全文件名
        safe = re.sub(r"[^A-Za-z0-9_.-]", "_", extra)
        return os.path.join(cache_dir, f"CIK{cik}_{kind}_{safe}.json")
    return os.path.join(cache_dir, f"CIK{cik}_{kind}.json")


def _read_cache(path: str, max_age_days: int = 7):
    """读取本地缓存, 如果文件不存在或已过期则返回 None"""
    if not USE_CACHE or not os.path.exists(path):
        return None
    try:
        mtime = os.path.getmtime(path)
        if (time.time() - mtime) > max_age_days * 86400:
            return None
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError, OSError):
        return None


def _write_cache(path: str, data):
    """写入本地缓存"""
    if not USE_CACHE:
        return
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
    except (IOError, OSError):
        pass  # 缓存写入失败不影响主流程


def _fetch_one_file_shard(file_entry: dict, start_year: int,
                          logger: logging.Logger = None) -> list:
    """拉取单个历史分片, 解析后返回 (year, form, ...) 列表"""
    file_url = file_entry.get("name")
    filing_from = file_entry.get("filingFrom", "")
    filing_to = file_entry.get("filingTo", "")

    # 早期跳过: 如果分片最大年份也 < start_year, 完全不需要请求
    try:
        if filing_to and int(filing_to[:4]) < start_year:
            return []
    except (ValueError, IndexError):
        pass

    if not file_url:
        return []

    # 尝试读缓存 (按 URL 的最后一段作为 key)
    cache_key = file_url.rsplit("/", 1)[-1].replace(".json", "")
    cache_p = _cache_path(file_url.split("/")[-3] if "/" in file_url else "x",
                          kind="shard", extra=cache_key)
    # 上面 cache_key 算出来可能不唯一, 这里用 file_url 的 hash 兜底
    import hashlib
    cache_key_hash = hashlib.md5(file_url.encode()).hexdigest()[:12]
    cache_p = _cache_path(cik="shard", kind=cache_key_hash, extra=cache_key)

    cached = _read_cache(cache_p, max_age_days=30)
    if cached is not None:
        return _parse_filing_records(cached, start_year)

    try:
        file_resp = retry_request("GET", file_url, HEADERS_DATA, logger=logger)
        file_resp.raise_for_status()
        file_data = file_resp.json()
    except Exception as e:
        if logger:
            logger.debug(f"分片请求失败 {file_url}: {e}")
        return []

    _write_cache(cache_p, file_data)
    return _parse_filing_records(file_data, start_year)


def _parse_filing_records(file_data: dict, start_year: int) -> list:
    """从分片 JSON 解析出目标年报记录列表"""
    forms = file_data.get("form", [])
    dates = file_data.get("filingDate", [])
    accs = file_data.get("accessionNumber", [])
    docs = file_data.get("primaryDocument", [])

    out = []
    for form, date_str, acc, doc in zip(forms, dates, accs, docs):
        try:
            filing_year = int(date_str.split("-")[0])
        except (ValueError, IndexError):
            continue
        if filing_year < start_year:
            continue
        form_norm = form.strip().upper()
        if form_norm in TARGET_FORMS:
            out.append({
                "year": filing_year,
                "form": form_norm,
                "filing_date": date_str,
                "accession": acc,
                "document": doc,
            })
    return out


def fetch_all_filings(cik: str, start_year: int = START_YEAR,
                      logger: logging.Logger = None) -> dict:
    """
    获取指定CIK公司从 start_year 起的所有 10-K / 20-F / N-CSR 提交记录
    优化点:
    1) 本地缓存: CIK{cik}.json 命中缓存则零网络请求
    2) files 分片并发: 用小线程池并发拉所有历史分片 (老公司可能有 10+ 个)
    3) 每个分片也有本地缓存
    4) 早分片跳过: filingTo < start_year 的分片直接跳过, 不发请求

    返回: {年份: [filing_dict, ...]} (内部已去重 + 按日期排序)
    """
    url = f"https://data.sec.gov/submissions/CIK{cik}.json"
    cache_p = _cache_path(cik, kind="submissions")

    # ---- 1) 读 CIK{cik}.json 缓存 ----
    data = _read_cache(cache_p, max_age_days=7)

    if data is None:
        # 没有缓存, 走网络
        try:
            resp = retry_request("GET", url, HEADERS_DATA, logger=logger)
            resp.raise_for_status()
            data = resp.json()
            _write_cache(cache_p, data)
        except Exception as e:
            if logger:
                logger.error(f"获取 {url} 失败: {e}")
            raise

    all_filings = []

    # ---- 2) 处理 recent 部分 ----
    recent = data.get("filings", {}).get("recent", {})
    if recent:
        all_filings.extend(_parse_filing_records(recent, start_year))

    # ---- 3) 并发处理 files 分片 ----
    files = data.get("filings", {}).get("files", [])
    if files:
        # 早期跳过: filingTo 年份 < start_year 的分片直接丢弃
        relevant_files = []
        for f_entry in files:
            filing_to = f_entry.get("filingTo", "")
            try:
                if filing_to and int(filing_to[:4]) < start_year:
                    continue
            except (ValueError, IndexError):
                pass
            relevant_files.append(f_entry)

        if relevant_files:
            # 用小线程池并发拉分片 (最多 4 个并发, 避免和主下载线程抢资源)
            with ThreadPoolExecutor(max_workers=min(4, len(relevant_files))) as ex:
                futures = [ex.submit(_fetch_one_file_shard, fe, start_year, logger)
                           for fe in relevant_files]
                for fut in as_completed(futures):
                    try:
                        all_filings.extend(fut.result())
                    except Exception as e:
                        if logger:
                            logger.debug(f"分片处理异常: {e}")

    # ---- 4) 按年份分组 + 去重 + 排序 ----
    yearly_filings = defaultdict(list)
    for f in all_filings:
        yearly_filings[f["year"]].append(f)

    for yr in yearly_filings:
        seen = set()
        unique = []
        # 按日期升序排, 同 accession 只留一份
        for f in sorted(yearly_filings[yr], key=lambda x: x["filing_date"]):
            if f["accession"] not in seen:
                seen.add(f["accession"])
                unique.append(f)
        yearly_filings[yr] = unique

    return yearly_filings


def select_best_filing(year_filings: list) -> dict:
    """
    从某年的所有符合条件的filing中选择最佳的一份（严格保证每年最多一份）:
    1) 优先级: 10-K > 10-K/A > 20-F > 20-F/A > N-CSR > N-CSR/A
    2) 同一 form 内如有多个 accession (重复提交), 取日期最新的那一份
    3) 找不到任何目标 form 时返回 None (由调用方安全跳过)
    """
    if not year_filings:
        return None
    for target_form in FORM_PRIORITY:
        candidates = [f for f in year_filings if f["form"] == target_form]
        if not candidates:
            continue
        # 同一 form 内再按 accession 去重 + 取日期最新
        seen = set()
        unique = []
        for f in sorted(candidates, key=lambda x: x["filing_date"]):
            if f["accession"] in seen:
                continue
            seen.add(f["accession"])
            unique.append(f)
        if unique:
            return unique[-1]  # 最新一份
    return None


def build_edgar_url(cik: str, accession: str):
    """根据accession号构建EDGAR文档的索引页URL"""
    acc_no_dash = accession.replace("-", "")
    cik_int = int(cik)
    dir_url = f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{acc_no_dash}/"
    return dir_url, acc_no_dash


def _head_check(url: str, session=None, logger=None) -> bool:
    """HEAD 请求快速验证 URL 是否存在 (200 + 足够大)"""
    try:
        sess = session or get_session()
        resp = sess.head(url, headers=HEADERS_EDGAR, timeout=15, allow_redirects=True)
        if resp.status_code == 200:
            cl = resp.headers.get("Content-Length")
            # 没有 Content-Length 头的, 视为可能可用
            if cl is None or int(cl) >= MIN_FILE_SIZE:
                return True
    except Exception:
        pass
    return False


def find_best_document_url(cik: str, accession: str, document_name: str,
                           logger: logging.Logger = None) -> tuple:
    """
    在 EDGAR 索引页中查找最佳文档URL。
    优化:
    1) primaryDocument 已知时, 按 (原扩展名, .html, .htm, .txt) 顺序逐个 HEAD 验证, 命中即返回
    2) 全部失败才请求 index 页 (大文件, 慢)
    3) index 页解析失败时, 用兜底 URL 列表再 HEAD 一遍

    返回: (下载URL, 文件扩展名) 或 (None, None)
    """
    dir_url, acc_no_dash = build_edgar_url(cik, accession)

    # ---- 优化1: 有 primaryDocument, 直接用 ----
    if document_name and document_name.strip():
        base_name = document_name.rsplit(".", 1)[0] if "." in document_name else document_name
        orig_ext = "." + document_name.rsplit(".", 1)[-1].lower() if "." in document_name else ""
        # 候选扩展名优先级: 原扩展名 (如果合理) > .html > .htm > .txt
        candidate_exts = []
        if orig_ext and orig_ext in (".html", ".htm", ".txt"):
            candidate_exts.append(orig_ext)
        for ext in (".html", ".htm", ".txt"):
            if ext not in candidate_exts:
                candidate_exts.append(ext)

        # 逐个 HEAD 验证 (优先用 .html 走快路径, SEC 几乎都是 html)
        for ext in candidate_exts:
            cand_url = dir_url + base_name + ext
            if _head_check(cand_url, logger=logger):
                if logger:
                    logger.debug(f"primaryDocument HEAD 命中: {cand_url}")
                return cand_url, ext

        # primaryDocument 全部扩展名都失败, 才回退到 index 页
        if logger:
            logger.debug(f"primaryDocument HEAD 全失败: {base_name}, 回退 index 页")

    # 优化2: 没有 primaryDocument 才请求 index 页
    index_url = dir_url + f"{acc_no_dash}-index.html"
    try:
        resp = retry_request("GET", index_url, HEADERS_EDGAR, logger=logger)
        if resp.status_code != 200:
            if logger:
                logger.debug(f"索引页{resp.status_code}: {index_url}")
            return _fallback_url(dir_url, acc_no_dash, document_name, logger)
        html_content = resp.text
    except Exception as e:
        if logger:
            logger.warning(f"获取索引页失败: {index_url} — {e}")
        return _fallback_url(dir_url, acc_no_dash, document_name, logger)

    # ---- 结构化解析：查找文档表格 ----
    # SEC index页面的标准结构是一个table，每行包含 <a> 链接
    # 先尝试用正则提取所有文档链接（按优先级分类）
    doc_links = []

    # 方法1: 精确匹配 — 链接href中包含accession号
    pattern_exact = rf'<a\s+[^>]*href="([^"]*{re.escape(acc_no_dash)}[^"]*\.(?:html?|txt))"'
    matches = re.findall(pattern_exact, html_content, re.IGNORECASE)
    doc_links.extend(matches)

    # 方法2: 宽松匹配 — 表格内所有文档链接
    if not doc_links:
        # 匹配表格行中的链接
        table_pattern = r'<tr[^>]*>.*?<a\s+[^>]*href="([^"]+\.(?:html?|txt))".*?</tr>'
        matches = re.findall(table_pattern, html_content, re.IGNORECASE | re.DOTALL)
        doc_links.extend(matches)

    # 方法3: 最宽松 — 所有.html/.htm/.txt链接
    if not doc_links:
        pattern_loose = r'<a\s+[^>]*href="([^"]+\.(?:html?|txt))"'
        matches = re.findall(pattern_loose, html_content, re.IGNORECASE)
        doc_links.extend(matches)

    # 分类链接
    html_links, htm_links, txt_links = [], [], []
    for link in doc_links:
        link_lower = link.lower()
        # 排除图片、CSS等
        if any(x in link_lower for x in (".jpg", ".png", ".gif", ".css", ".js", ".xml", ".xsd")):
            continue
        if link_lower.endswith(".html"):
            html_links.append(link)
        elif link_lower.endswith(".htm"):
            htm_links.append(link)
        elif link_lower.endswith(".txt"):
            txt_links.append(link)

    # 按优先级选择：html > htm > txt，优先含accession号的
    for candidates, ext in [(html_links, ".html"), (htm_links, ".htm"), (txt_links, ".txt")]:
        if not candidates:
            continue
        # 优先选文件名含accession号的
        best = None
        for c in candidates:
            if acc_no_dash in c:
                best = c
                break
        if best is None:
            # 次优先：选主文档（排除exhibit附件等）
            for c in candidates:
                c_upper = c.upper()
                if "EXHIBIT" not in c_upper and "EX-" not in c_upper and "EX99" not in c_upper:
                    best = c
                    break
        if best is None:
            best = candidates[0]

        # 构建完整URL
        full_url = best if best.startswith("http") else dir_url + best
        if logger:
            logger.debug(f"选定文档URL: {full_url}")
        return full_url, ext

    # 完全匹配失败，使用兜底逻辑
    return _fallback_url(dir_url, acc_no_dash, document_name, logger)


def _fallback_url(dir_url: str, acc_no_dash: str, document_name: str,
                  logger: logging.Logger = None) -> tuple:
    """
    兜底URL构建逻辑（当index页面解析失败时）
    按优先级返回 .html > .htm > .txt 的猜测URL
    """
    if logger:
        logger.debug(f"使用兜底URL构建: {dir_url}")

    if document_name and document_name.strip():
        base_name = document_name.rsplit(".", 1)[0] if "." in document_name else document_name
        best_ext = ".html"  # 优先html
        candidate = dir_url + base_name + best_ext
        if logger:
            logger.debug(f"兜底URL: {candidate}")
        return candidate, best_ext

    # 没有document_name，用accession号构建，优先html
    candidate = dir_url + acc_no_dash + ".html"
    return candidate, ".html"


# ============================================================
#                    下载层
# ============================================================


def download_filing(cik: str, filing_info: dict, company_dir: str,
                    logger: logging.Logger = None) -> bool:
    """
    下载单份年报文件（带重试和完整性检查）
    返回: True(成功/已存在) / False(失败)
    """
    year = filing_info["year"]
    form_type = filing_info["form"]
    filing_date = filing_info["filing_date"]
    accession = filing_info["accession"]
    document = filing_info["document"]

    # 查找最佳文档URL
    doc_url, ext = find_best_document_url(cik, accession, document, logger)
    if not doc_url:
        if logger:
            logger.warning(f"无法构建下载URL: CIK={cik}, year={year}")
        return False

    # 构建安全的文件名
    ticker_name = os.path.basename(company_dir)
    safe_date = filing_date.replace("-", "")
    safe_form = sanitize_filename(form_type)  # 10-K/A → 10-K_A
    filename = f"{ticker_name}_{year}_{safe_form}_{safe_date}{ext}"
    filepath = os.path.join(company_dir, filename)

    # 如果已存在且文件大小正常，跳过
    if os.path.exists(filepath) and os.path.getsize(filepath) >= MIN_FILE_SIZE:
        if logger:
            logger.debug(f"已存在，跳过: {filepath}")
        return True

    # 下载（带重试）
    for attempt in range(MAX_RETRIES + 1):
        try:
            resp = retry_request("GET", doc_url, HEADERS_EDGAR, timeout=REQUEST_TIMEOUT, logger=logger)

            if resp.status_code == 200 and len(resp.content) >= MIN_FILE_SIZE:
                # 验证是否为HTML/TXT内容（排除错误页面）
                content_preview = resp.content[:500]
                # 检查是否是SEC错误页面
                if b"<title>SEC.gov | " not in content_preview or b"Request Rate Threshold Exceeded" not in content_preview:
                    with open(filepath, "wb") as f:
                        f.write(resp.content)
                    if logger:
                        logger.info(f"下载成功: {filename} ({len(resp.content)} bytes)")
                    return True
                else:
                    if logger:
                        logger.warning(f"SEC错误页面: {doc_url}")
                    time.sleep(BASE_BACKOFF * (2 ** attempt))
                    continue

            # HTML/HTM失败时尝试TXT
            if ext.lower() in (".html", ".htm"):
                txt_url = doc_url.rsplit(".", 1)[0] + ".txt"
                try:
                    resp_txt = retry_request("GET", txt_url, HEADERS_EDGAR, timeout=REQUEST_TIMEOUT, logger=logger)
                    if resp_txt.status_code == 200 and len(resp_txt.content) >= MIN_FILE_SIZE:
                        txt_filepath = filepath.rsplit(".", 1)[0] + ".txt"
                        with open(txt_filepath, "wb") as f:
                            f.write(resp_txt.content)
                        if logger:
                            logger.info(f"下载成功(TXT兜底): {os.path.basename(txt_filepath)} ({len(resp_txt.content)} bytes)")
                        return True
                except Exception:
                    pass

            if attempt < MAX_RETRIES:
                wait = BASE_BACKOFF * (2 ** attempt)
                if logger:
                    logger.warning(f"下载失败 {doc_url} (HTTP {resp.status_code})，重试 {attempt+1}/{MAX_RETRIES}")
                time.sleep(wait)
            else:
                if logger:
                    logger.error(f"下载最终失败: {doc_url} (HTTP {resp.status_code})")
                return False

        except Exception as e:
            if attempt < MAX_RETRIES:
                wait = BASE_BACKOFF * (2 ** attempt)
                if logger:
                    logger.warning(f"下载异常 {doc_url}: {e}，重试 {attempt+1}/{MAX_RETRIES}")
                time.sleep(wait)
            else:
                if logger:
                    logger.error(f"下载最终失败: {doc_url} — {e}")
                return False

    return False


def process_ticker(ticker: str, ticker_to_cik: dict, progress: dict,
                   logger: logging.Logger) -> tuple:
    """
    处理单个ticker的完整流程（获取filing列表 + 下载）
    返回: (ticker, success_count, fail_count, skipped_years)
    """
    ticker_upper = ticker.upper()

    # 顶层 try/except 兜底: 任何意外都不能卡死线程池
    try:
        return _process_ticker_inner(ticker, ticker_to_cik, progress, logger)
    except Exception as e:
        safe_print(f"  {ticker}: 处理严重异常 — {e}")
        if logger:
            logger.error(f"{ticker}: 顶层异常 — {e}")
        progress[ticker_upper] = f"error:{type(e).__name__}"
        return (ticker, 0, 1, 0)


def _process_ticker_inner(ticker: str, ticker_to_cik: dict, progress: dict,
                          logger: logging.Logger) -> tuple:
    """process_ticker 的实际实现, 由外层 try/except 包裹"""
    ticker_upper = ticker.upper()

    # 检查是否已完成（断点续传）
    if progress.get(ticker_upper) == "done":
        safe_print(f"  {ticker}: 已完成，跳过")
        return (ticker, 0, 0, 0)

    if ticker_upper not in ticker_to_cik:
        safe_print(f"  {ticker}: 未找到CIK，跳过")
        progress[ticker_upper] = "no_cik"
        return (ticker, 0, 1, 0)

    cik = ticker_to_cik[ticker_upper]
    company_dir = os.path.join(SAVE_ROOT, ticker_upper)
    os.makedirs(company_dir, exist_ok=True)

    # 获取filing列表
    try:
        yearly_filings = fetch_all_filings(cik, START_YEAR, logger)
    except Exception as e:
        safe_print(f"  {ticker}: 获取提交记录失败 — {e}")
        logger.error(f"{ticker}: 获取提交记录失败 — {e}")
        progress[ticker_upper] = f"error_fetch:{e}"
        return (ticker, 0, 1, 0)

    if not yearly_filings:
        safe_print(f"  {ticker}: 无{START_YEAR}年后的年报记录")
        progress[ticker_upper] = "done"
        return (ticker, 0, 0, 0)

    # 每年选最佳的一份
    selected_filings = []
    for yr in sorted(yearly_filings.keys()):
        best = select_best_filing(yearly_filings[yr])
        if best:
            selected_filings.append(best)

    if not selected_filings:
        # 完全没找到任何目标年报（如基金型公司）—— 安全跳过
        safe_print(f"  {ticker}: 未找到 10-K/20-F/N-CSR 年报，跳过")
        progress[ticker_upper] = "done"
        return (ticker, 0, 0, 0)

    # 下载 (单份失败不卡死, 走下一年)
    company_success = 0
    company_fail = 0
    for filing in selected_filings:
        try:
            if download_filing(cik, filing, company_dir, logger):
                company_success += 1
            else:
                company_fail += 1
        except Exception as e:
            company_fail += 1
            if logger:
                logger.error(f"{ticker} {filing.get('year')} 异常跳过: {e}")
        # 公司内下载间隔 (自适应, 限流时自动加长)
        rate_limited_sleep()

    # 记录结果
    progress[ticker_upper] = "done"
    years_covered = ", ".join(str(f["year"]) for f in selected_filings)
    form_types = ", ".join(sorted({f["form"] for f in selected_filings}))
    logger.info(
        f"{ticker}: {company_success}/{len(selected_filings)} 下载成功 "
        f"(年份: {years_covered}; 表单: {form_types})"
    )
    safe_print(
        f"  {ticker}: {company_success}/{len(selected_filings)} 份年报下载成功 "
        f"({', '.join(str(f['year']) for f in selected_filings)})"
    )

    return (ticker, company_success, company_fail, 0)


# ============================================================
#                       主函数
# ============================================================


def main():
    """主函数 — 协调整个下载流程"""
    print("=" * 60)
    print("SEC 年报下载工具 (10-K / 20-F / N-CSR) — 高稳定性版本")
    print("=" * 60)

    # 1. 初始化日志
    log_path = LOG_PATH or os.path.join(SAVE_ROOT, "download.log")
    logger = setup_logging(log_path)
    logger.info("=" * 50)
    logger.info("SEC 年报下载工具启动")
    logger.info(f"CSV: {CSV_PATH}")
    logger.info(f"保存路径: {SAVE_ROOT}")
    logger.info(f"并发线程数: {MAX_WORKERS}")
    logger.info(f"起始年份: {START_YEAR}")

    # 2. 检查CSV文件
    if not os.path.exists(CSV_PATH):
        msg = f"CSV文件不存在: {CSV_PATH}"
        print(f"\n[错误] {msg}")
        logger.error(msg)
        return

    # 3. 加载ticker列表
    print(f"\n[1/5] 从CSV加载ticker列表: {CSV_PATH}")
    try:
        tickers = load_tickers_from_csv(CSV_PATH)
    except Exception as e:
        msg = f"读取CSV失败: {e}"
        print(f"[错误] {msg}")
        logger.error(msg)
        return

    if not tickers:
        print("[错误] 未能从CSV中读取到任何ticker")
        logger.error("CSV中无ticker数据")
        return
    print(f"  共加载 {len(tickers)} 个ticker")
    logger.info(f"加载 {len(tickers)} 个ticker")

    # 4. 获取CIK映射
    print("\n[2/5] 获取SEC CIK映射表...")
    try:
        ticker_to_cik = get_ticker_cik_mapping(logger)
        print(f"  获取成功，共 {len(ticker_to_cik)} 条映射")
        logger.info(f"CIK映射获取成功: {len(ticker_to_cik)} 条")
    except Exception as e:
        msg = f"获取CIK映射失败: {e}"
        print(f"[错误] {msg}")
        logger.error(msg)
        return
    time.sleep(0.5)

    # 5. 加载断点续传记录
    progress_path = PROGRESS_PATH or os.path.join(SAVE_ROOT, "progress.json")
    progress = load_progress(progress_path)
    completed_count = sum(1 for v in progress.values() if v == "done")
    if completed_count > 0:
        print(f"  检测到 {completed_count} 家已完成，将跳过")
        logger.info(f"断点续传: {completed_count} 家已完成")

    # 6. 创建保存目录
    os.makedirs(SAVE_ROOT, exist_ok=True)
    print(f"\n[3/5] 年报保存路径: {SAVE_ROOT}")
    print(f"[4/5] 日志文件: {log_path}")
    print(f"\n[5/5] 开始下载年报 (并发线程数: {MAX_WORKERS})...")
    print("-" * 60)

    total_success = 0
    total_fail = 0
    total_skipped = 0

    # 使用线程池并发处理（顺序提交，有限并发）
    if MAX_WORKERS > 1:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {}
            for ticker in tickers:
                future = executor.submit(
                    process_ticker, ticker, ticker_to_cik, progress, logger
                )
                futures[future] = ticker

            # 使用tqdm显示进度
            with tqdm(total=len(futures), desc="整体进度") as pbar:
                for future in as_completed(futures):
                    ticker = futures[future]
                    try:
                        _, s, f, skip = future.result()
                        total_success += s
                        total_fail += f
                        total_skipped += skip
                    except Exception as e:
                        safe_print(f"  {ticker}: 处理异常 — {e}")
                        logger.error(f"{ticker}: 处理异常 — {e}")
                        total_fail += 1

                    pbar.update(1)
                    # 定期保存进度
                    if pbar.n % 10 == 0:
                        save_progress(progress, progress_path)

    else:
        # 单线程模式
        for ticker in tqdm(tickers, desc="整体进度"):
            try:
                _, s, f, skip = process_ticker(ticker, ticker_to_cik, progress, logger)
                total_success += s
                total_fail += f
                total_skipped += skip
            except Exception as e:
                safe_print(f"  {ticker}: 处理异常 — {e}")
                logger.error(f"{ticker}: 处理异常 — {e}")
                total_fail += 1
            # 定期保存进度
            save_progress(progress, progress_path)

    # 最终保存进度
    save_progress(progress, progress_path)

    # 汇总
    print("\n" + "=" * 60)
    print("全部任务完成！")
    print(f"  成功: {total_success} 份")
    print(f"  失败: {total_fail} 份")
    print(f"  跳过: {total_skipped} 家公司")
    print(f"  年报保存在: {SAVE_ROOT}")
    print(f"  日志文件: {log_path}")
    print("=" * 60)

    logger.info("=" * 50)
    logger.info(f"任务完成 — 成功:{total_success} 失败:{total_fail} 跳过:{total_skipped}")
    logger.info("=" * 50)


if __name__ == "__main__":
    main()
