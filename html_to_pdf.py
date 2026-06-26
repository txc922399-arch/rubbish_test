import asyncio
import os
from pathlib import Path
from playwright.async_api import async_playwright


async def html_to_pdf(input_dir, output_dir):
    input_path = Path(input_dir)
    output_path = Path(output_dir)
    
    output_path.mkdir(parents=True, exist_ok=True)
    
    html_files = list(input_path.glob("*.htm")) + list(input_path.glob("*.html"))
    
    if not html_files:
        print(f"No HTML/HTM files found in {input_dir}")
        return
    
    print(f"Found {len(html_files)} HTML/HTM files")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        for html_file in html_files:
            pdf_filename = html_file.stem + ".pdf"
            pdf_filepath = output_path / pdf_filename
            
            file_url = f"file:///{html_file.absolute().as_posix()}"
            
            print(f"Converting: {html_file.name} -> {pdf_filename}")
            
            try:
                await page.goto(file_url, wait_until="networkidle")
                
                await page.pdf(
                    path=pdf_filepath,
                    format="A4",
                    print_background=True
                )
                
                print(f"Successfully created: {pdf_filename}")
            except Exception as e:
                print(f"Error converting {html_file.name}: {e}")
        
        await browser.close()
    
    print("All conversions completed!")


if __name__ == "__main__":
    INPUT_DIR = r"D:\working_documents\test\US_listed_companies\10K_filings\ACU"
    OUTPUT_DIR = r"D:\working_documents\test\playwright_pdf\text"
    
    asyncio.run(html_to_pdf(INPUT_DIR, OUTPUT_DIR))
