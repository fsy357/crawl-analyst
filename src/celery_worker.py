"""Celery 异步任务层

为什么需要它：
- 爬 arXiv 一页要 20~60 秒，LIDA 调一次大模型要 20~40 秒，两者加起来接近一分半。
  放在 HTTP 请求里同步执行，用户点一下按钮要卡住，前端会超时。
- 所以用 Celery + Redis 把重活扔到后台：接口只负责"提交任务 + 返回 task_id"，
  前端拿着 task_id 轮询进度。这是从"能跑的脚本"变成"能用的服务"的关键一步。

Windows 注意：必须用 `--pool=solo`，默认的 prefork 依赖 fork，Windows 上没有。
solo 是单进程串行执行，性能不如下面 Linux 的 prefork，但足够演示；
真正部署到 Linux 容器里可以去掉 --pool=solo 换成并发。

任务职责划分：
- `crawl.collect`  只采集+清洗+增量入库，不调大模型（便宜、快）
- `crawl.analyze`  只读库做分析出图（贵、慢）
- `crawl.pipeline` 先 collect，只有确实有新数据才继续 analyze（省钱的核心）
"""
import asyncio
import os

from celery import Celery
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(BASE_DIR, ".env"))

REDIS_URL = os.getenv("REDIS_URL")
if not REDIS_URL:
    raise RuntimeError("REDIS_URL 未配置，请在 .env 里填 redis://:密码@127.0.0.1:6379/0")

DEFAULT_GOAL = "按团队规模统计论文数量（1人/2-4人/5-8人/9人以上）"

app = Celery("crawl_analyst", broker=REDIS_URL, backend=REDIS_URL)
app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="Asia/Shanghai",
    enable_utc=False,
    task_track_started=True,      # 让前端能看到 STARTED 状态
    task_time_limit=1800,         # 单个任务最长 30 分钟
    task_soft_time_limit=1500,
    worker_max_tasks_per_child=50,  # 防止爬虫/浏览器内存泄漏累积
    broker_connection_retry_on_startup=True,
    result_expires=86400,         # 结果保留 1 天
)


@app.task(bind=True, name="crawl.collect", max_retries=2, default_retry_delay=60)
def collect_task(self, category: str = "cs.AI", max_entries: int = 100):
    """采集 + 清洗 + 增量入库"""
    from src.monitor_svc import watch_round

    self.update_state(state="PROGRESS", meta={"stage": "crawling", "category": category})
    try:
        result = asyncio.run(watch_round(category, max_entries))
    except Exception as e:
        # 网络抖动导致的失败重试两次，仍然失败就抛出，由前端展示 error
        raise self.retry(exc=e)
    return result


@app.task(bind=True, name="crawl.analyze")
def analyze_task(self, category: str = "cs.AI", goal: str = DEFAULT_GOAL, task_id: str = "manual"):
    """从库里读数据 → LIDA 分析出图"""
    from src.db import load_papers, save_analysis
    from src.lida_svc import analyze_dataframe

    self.update_state(state="PROGRESS", meta={"stage": "analyzing", "category": category})
    df = load_papers(category=category)
    if df.empty:
        return {"ok": False, "error": "库里没有该分类的数据，请先采集"}

    summary, charts = analyze_dataframe(df, goal=goal, library="seaborn", prefix=task_id)
    save_analysis(task_id, goal, charts)
    return {
        "ok": True,
        "rows": int(len(df)),
        "goal": goal,
        "charts": charts,
        "fields": summary.get("fields"),
    }


@app.task(bind=True, name="page.pipeline")
def page_task(self, url: str, goal: str = None):
    """通用网址流水线：抓取 → 自动抽结构 → 摘要 → 自动/指定分析目标 → 出图"""
    from src.page_svc import run_page

    self.update_state(state="PROGRESS", meta={"stage": "crawling", "url": url})
    result = asyncio.run(run_page(url, goal))

    if result.get("ok"):
        self.update_state(state="PROGRESS", meta={"stage": "done", "page_id": result["page_id"]})
    return result


@app.task(bind=True, name="crawl.pipeline")
def pipeline_task(self, category: str = "cs.AI", max_entries: int = 100, goal: str = DEFAULT_GOAL):
    """采集 → 判断是否需要分析 → 需要就分析。一条命令跑完整条链路。"""
    from src.monitor_svc import watch_round
    from src.db import load_papers, save_analysis
    from src.lida_svc import analyze_dataframe

    self.update_state(state="PROGRESS", meta={"stage": "crawling", "category": category})
    collect_result = asyncio.run(watch_round(category, max_entries))

    # 页面没变化且没有新数据时，跳过昂贵的大模型分析
    if not collect_result.get("needs_analysis"):
        return {"ok": True, "skipped_analysis": True, "collect": collect_result}

    self.update_state(state="PROGRESS", meta={"stage": "analyzing", "category": category})
    df = load_papers(category=category)
    summary, charts = analyze_dataframe(
        df, goal=goal, library="seaborn", prefix=collect_result["task_id"]
    )
    save_analysis(collect_result["task_id"], goal, charts)
    return {
        "ok": True,
        "skipped_analysis": False,
        "collect": collect_result,
        "charts": charts,
        "rows": int(len(df)),
        "fields": summary.get("fields"),
    }
