# Celery 链路测试：提交任务 → 轮询结果
import sys
sys.path.insert(0, "D:/crawl_analyst")

import time
from src.celery_worker import app, collect_task, analyze_task


def wait(result, timeout=300, label=""):
    start = time.time()
    last_state = None
    while time.time() - start < timeout:
        state = result.state
        if state != last_state:
            print(f"[{label}] 状态: {state}")
            last_state = state
        if result.ready():
            return result.get()
        time.sleep(2)
    raise TimeoutError(f"{label} 超时")


def main():
    print("broker:", app.conf.broker_url.split("@")[-1])

    print("\n=== 提交采集任务 ===")
    r = collect_task.delay("cs.AI", 100)
    print("task_id:", r.id)
    out = wait(r, timeout=300, label="collect")
    print("结果: changed=%s 新增=%s 总数=%s 需要分析=%s" % (
        out["changed"], out["new_rows"], out["total"], out["needs_analysis"]))

    print("\n=== 提交分析任务 ===")
    r2 = analyze_task.delay("cs.AI", "按团队规模统计论文数量（1人/2-4人/5-8人/9人以上）", "celerytest")
    print("task_id:", r2.id)
    out2 = wait(r2, timeout=300, label="analyze")
    print("结果: ok=%s 行数=%s 图表数=%s" % (out2["ok"], out2.get("rows"), len(out2.get("charts", []))))
    for c in out2.get("charts", []):
        print("  图:", c["image_path"], "状态:", c["status"])


if __name__ == "__main__":
    main()
