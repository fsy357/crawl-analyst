# 完整链路测试：arXiv 采集 → 清洗 → DuckDB 入库 → LIDA 分析出图
import sys
sys.path.insert(0, "D:/crawl_analyst")

import asyncio
import json
from src.db import init_db, save_analysis
from src.crawl_svc import run_collect
from src.lida_svc import analyze_dataframe


async def main():
    print("=== 1) 初始化数据库 ===")
    print("db:", init_db())

    print("\n=== 2) 采集 cs.AI 最近论文（100 条） ===")
    info = await run_collect("cs.AI", 100)
    print("任务ID:", info["task_id"])
    print("抓取URL:", info["urls"])
    print("成功页数:", info["fetched_pages"], "| 失败URL:", info["failed_urls"])
    print("原始 markdown:", info["raw_md_path"])
    print("原始行数:", info["rows_raw"], "→ 清洗后行数:", info["rows_clean"], "→ 库内总数:", info["total_in_db"])
    print("清洗报告:")
    print(json.dumps(info["wash_report"], ensure_ascii=False, indent=2))

    print("\n=== 3) 从库里读出来做分析 ===")
    from src.db import load_papers
    df = load_papers(category="cs.AI")
    print("读到:", df.shape)
    print(df[["paper_id", "month", "author_count", "title_len", "has_code"]].head(5).to_string(index=False))

    print("\n=== 4) LIDA 分析 ===")
    profile, charts = analyze_dataframe(
        df,
        goal="按月统计 arXiv 论文数量，画一张折线图看趋势；并在图上标注标题里带 Agent 的论文占比",
        library="seaborn",
    )
    for i, c in enumerate(charts):
        print(f"图表{i} 成功={c['status']} 图片={c['image_path']}")
        if c["error"]:
            print("报错:", str(c["error"])[:200])
    save_analysis(info["task_id"], "月度趋势分析", charts)
    print("\n分析结果已入库")


if __name__ == "__main__":
    asyncio.run(main())
