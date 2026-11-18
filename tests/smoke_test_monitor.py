# 增量监控测试：验证“无变化跳过分析、有新数据只入库新增”
import sys
sys.path.insert(0, "D:/crawl_analyst")

import asyncio
import json
from src.db import init_db, get_con, count_papers
from src.monitor_svc import watch_round


def delete_some(limit=3):
    """模拟数据被删（或上游数据源发生变化），验证差集能补回来"""
    con = get_con()
    ids = con.execute(f"SELECT paper_id FROM papers LIMIT {limit}").fetchdf()["paper_id"].tolist()
    con.execute(f"DELETE FROM papers WHERE paper_id IN ({', '.join(['?'] * len(ids))})", ids)
    con.close()
    return ids


async def main():
    print("=== 初始化 ===")
    init_db()
    print("库内初始条数:", count_papers("cs.AI"))

    print("\n=== 第 1 轮：首次监控 ===")
    r1 = await watch_round("cs.AI", 100)
    print(json.dumps({k: r1[k] for k in ["changed", "new_rows", "total", "needs_analysis", "check_count"]}, ensure_ascii=False))

    print("\n=== 第 2 轮：页面没变化（应跳过分析）===")
    r2 = await watch_round("cs.AI", 100)
    print(json.dumps({k: r2[k] for k in ["changed", "new_rows", "total", "needs_analysis", "check_count"]}, ensure_ascii=False))
    print("指纹是否一致:", r1["fingerprint"] == r2["fingerprint"])

    print("\n=== 第 3 轮：模拟库里少了 3 条，验证差集补数据 ===")
    removed = delete_some(3)
    print("手动删除:", removed, "→ 当前条数:", count_papers("cs.AI"))
    r3 = await watch_round("cs.AI", 100)
    print(json.dumps({k: r3[k] for k in ["changed", "new_rows", "total", "needs_analysis", "check_count"]}, ensure_ascii=False))

    print("\n=== 结论 ===")
    print("第1轮 首次应有更新:", r1["changed"] is True)
    print("第2轮 无变化且不分析:", (r2["changed"] is False) and (r2["needs_analysis"] is False))
    print("第3轮 补回3条且要求重新分析:", (r3["new_rows"] == 3) and (r3["needs_analysis"] is True))
    print("最终库内条数:", count_papers("cs.AI"))


if __name__ == "__main__":
    asyncio.run(main())
