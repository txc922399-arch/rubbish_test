import os
import asyncio
from pathlib import Path
from playwright.async_api import async_playwright

# 配置路径
INPUT_DIR = r"D:\working_documents\test\US_listed_companies\10K_filings\ACU"
OUTPUT_DIR = r"D:\working_documents\test\playwright_pdf\text"

async def batch_html_to_pdf():
    # 自动创建输出目录
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # 筛选目录下所有 html/htm 文件
    html_files = [
        f for f in os.listdir(INPUT_DIR)
        if f.lower().endswith((".html", ".htm"))
    ]

    if not html_files:
        print("输入目录下未找到 HTML/HTM 文件")
        return

    print(f"共找到 {len(html_files)} 个文件，开始批量转换...\n")

    async with async_playwright() as p:
        # 启动无头浏览器
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(viewport={"width": 1200, "height": 900})

        success_count = 0
        for index, filename in enumerate(html_files, 1):
            input_full_path = os.path.join(INPUT_DIR, filename)
            # 生成输出 PDF 文件名
            pdf_name = os.path.splitext(filename)[0] + ".pdf"
            output_full_path = os.path.join(OUTPUT_DIR, pdf_name)

            try:
                page = await context.new_page()
                # 本地文件转 file 协议路径
                file_url = f"file:///{input_full_path.replace(os.sep, '/')}"
                
                # 等待页面完全加载（网络空闲）
                await page.goto(file_url, wait_until="networkidle", timeout=60000)
                
                # 生成 PDF，保留背景、A4 尺寸、设置边距
                await page.pdf(
                    path=output_full_path,
                    format="A4",
                    print_background=True,
                    margin={"top": "15px", "bottom": "15px", "left": "15px", "right": "15px"}
                )
                
                await page.close()
                success_count += 1
                print(f"[{index}/{len(html_files)}] 成功：{filename}")

            except Exception as e:
                print(f"[{index}/{len(html_files)}] 失败：{filename} | 错误：{str(e)}")
                try:
                    await page.close()
                except:
                    pass

        await browser.close()
        print(f"\n转换完成：成功 {success_count} 个，失败 {len(html_files)-success_count} 个")
        print(f"PDF 文件已输出至：{OUTPUT_DIR}")

if __name__ == "__main__":
    asyncio.run(batch_html_to_pdf())