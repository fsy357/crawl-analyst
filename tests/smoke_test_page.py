"""通用网址链路测试：python smoke_test_page.py <url> [可选分析目标]"""
import sys
sys.path.insert(0, "D:/crawl_analyst")

import asyncio
import json
from src.db import init_db
from src.page_svc import run_page


async def main():
    url = sys.argv[1] if len(sys.argv) > 1 else "https://books.toscrape.com/"
    goal = sys.argv[2] if len(sys.argv) > 2 else None
    init_db()
    r = await run_page(url, goal)
    print("成功:", r["ok"])
    if not r["ok"]:
        print("错误:", r.get("error"))
        return
    print("适配器:", r["adapter"], "| 行数:", r["rows"], "| 列:", r["cols"])
    print("分析目标:", r["goals"])
    print("图表:", [c["image_path"] for c in r["charts"]], "| 说明:", r["chart_error"])
    print("摘要前 300 字:\n" + (r["summary"] or "")[:300])


if __name__ == "__main__":
    asyncio.run(main())
