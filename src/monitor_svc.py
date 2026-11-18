"""增量监控层：判断数据源有没有变化，只入库新增的论文

核心思路（"实时更新"的实现方式）：
1. **内容指纹**：把这一页所有 paper_id 排序后做 sha256。指纹只和"内容"有关，
   和页面渲染、时间戳、广告位无关，所以不会误判成"变了"。
2. **指纹比对**：和 watch_state 表里上一次的指纹比较，相等说明页面没变化。
3. **主键差集**：不管指纹变没变，都拿本次抓到的 paper_id 和库里已有的做差集，
   只 INSERT 新增的行。这样即使指纹判断失误，也不会重复入库或漏数据。
4. **按需分析**：只有"页面变了"或"有新数据"时才触发 LIDA（大模型调用是要花钱的），
   页面没变化就跳过分析，省钱也省时间。

为什么要指纹 + 差集两套机制：
- 指纹是"要不要重新分析"的开关，成本低（一次 sha256）
- 差集是"数据准不准"的保险，成本也低（一次集合运算）
- 两者独立，任何一个失效都不会污染数据
"""
import argparse
import asyncio
import hashlib
import time
import uuid
from datetime import datetime

from src.crawl_svc import crawl_arxiv, clean_papers, build_list_urls
from src.db import (
    init_db,
    create_task,
    update_task,
    get_existing_ids,
    get_watch_state,
    save_watch_state,
    save_raw_markdown,
    upsert_papers,
    count_papers,
)


def content_fingerprint(paper_ids) -> str:
    """内容指纹：paper_id 排序后拼接做 sha256"""
    key = "\n".join(sorted(set(str(p) for p in paper_ids)))
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:32]


async def watch_round(category: str = "cs.AI", max_entries: int = 100) -> dict:
    """跑一轮监控：采集 → 指纹比对 → 差集 → 只入库新增 → 返回是否需要重新分析"""
    init_db()
    task_id = uuid.uuid4().hex[:12]
    watch_key = f"{category}:{max_entries}"
    start_url = build_list_urls(category, max_entries)[0]
    create_task(task_id, start_url, category, status="checking")

    try:
        raw_df, raw_md, crawl_meta = await crawl_arxiv(category, max_entries)
        if raw_df.empty:
            update_task(task_id, status="failed", error="本轮没有抓到任何数据")
            return {
                "task_id": task_id, "category": category, "ok": False,
                "changed": False, "new_rows": 0, "total": count_papers(category),
                "needs_analysis": False, "failed_urls": crawl_meta["failed_urls"],
                "wash_report": {}, "fingerprint": None,
            }

        fp = content_fingerprint(raw_df["raw_id"])
        prev = get_watch_state(watch_key)
        changed = (prev is None) or (prev["fingerprint"] != fp)
        check_count = (prev["check_count"] if prev else 0) + 1

        clean_df, report = clean_papers(raw_df, category=category)

        existing = get_existing_ids(category)
        new_df = clean_df[~clean_df["paper_id"].isin(existing)]

        if changed:
            md_path = save_raw_markdown(task_id, raw_md)
        else:
            md_path = None

        upsert_papers(new_df) if len(new_df) else None
        total = count_papers(category)

        update_task(
            task_id,
            status="done",
            raw_md_path=md_path,
            rows_kept=len(clean_df),
            new_rows=len(new_df),
            fingerprint=fp,
        )
        save_watch_state(
            watch_key=watch_key, category=category, fingerprint=fp,
            paper_count=len(clean_df), total=total, new_rows=len(new_df),
            check_count=check_count,
        )

        needs_analysis = changed or len(new_df) > 0
        return {
            "task_id": task_id,
            "category": category,
            "ok": True,
            "changed": changed,
            "new_rows": len(new_df),
            "total": total,
            "needs_analysis": needs_analysis,
            "check_count": check_count,
            "fingerprint": fp,
            "failed_urls": crawl_meta["failed_urls"],
            "wash_report": report,
        }
    except Exception as e:
        update_task(task_id, status="failed", error=f"{type(e).__name__}: {e}")
        raise


async def watch_loop(categories, interval_sec: int = 600, rounds: int = None, verbose: bool = True):
    """本地循环监控：按间隔轮流检查多个分类。

    每轮只做数据采集与入库；需要分析时通过回调交给上层（Celery / FastAPI），
    避免监控线程里直接跑大模型调用。
    """
    rounds_done = 0
    while True:
        for cat in categories:
            result = await watch_round(cat, max_entries=100)
            rounds_done += 1
            if verbose:
                flag = "有更新" if result["changed"] else "无变化"
                print(
                    f"[{datetime.now():%H:%M:%S}] {cat} | {flag} | "
                    f"新增 {result['new_rows']} 条 | 库内 {result['total']} 条 | "
                    f"需要重新分析: {result['needs_analysis']}"
                )
        if rounds and rounds_done >= rounds:
            break
        await asyncio.sleep(interval_sec)


def main():
    parser = argparse.ArgumentParser(description="arXiv 增量监控")
    parser.add_argument("--category", default="cs.AI")
    parser.add_argument("--rounds", type=int, default=1, help="检查几轮后退出，不填则一直跑")
    parser.add_argument("--interval", type=int, default=600, help="每轮间隔秒数")
    args = parser.parse_args()

    cats = [c.strip() for c in args.category.split(",") if c.strip()]
    asyncio.run(watch_loop(cats, interval_sec=args.interval, rounds=args.rounds))


if __name__ == "__main__":
    main()
