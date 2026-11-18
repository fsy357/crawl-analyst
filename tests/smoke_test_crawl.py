# Crawl4AI 冒烟测试：验证本地浏览器爬取是否正常
import asyncio
from crawl4ai import AsyncWebCrawler

async def main():
    async with AsyncWebCrawler(verbose=False) as crawler:
        result = await crawler.arun(url="https://books.toscrape.com")
        print("是否成功:", result.success)
        md = result.markdown or ""
        print("markdown 长度:", len(md))
        print("---- 前 300 字 ----")
        print(md[:300])

if __name__ == "__main__":
    asyncio.run(main())
