"""FastAPI 服务层：把采集和分析包装成 HTTP 接口 + SSE 实时进度

接口设计思路（面试讲点）：
- **提交与执行分离**：POST 只负责把任务丢进 Celery 队列并立刻返回 task_id，
  绝不在请求里同步跑爬虫（一页 20~60 秒，前端早超时了）。
- **进度用 SSE 推送**：`GET /api/stream/{task_id}` 长连接推 stage 变化，
  比前端每秒轮询更省资源，也比 WebSocket 简单（不需要双向通信）。
- **数据与分析分两个接口**：`/api/papers` 读事实表，`/api/analyze` 触发大模型，
  成本差异大，分开便于分别做限流和缓存。
"""
import os
import json
import asyncio
import hashlib
from datetime import datetime
from urllib.parse import urlparse

import pandas as pd
from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from celery.result import AsyncResult

from src.db import (init_db, load_papers, count_papers, get_recent_tasks,
                    get_con, get_page, list_pages, count_pages,
                    save_watch, list_watches, get_watch, touch_watch, delete_watch)
from src.celery_worker import app as celery_app, collect_task, analyze_task, pipeline_task, page_task
from src.lida_svc import CHART_DIR
from src.crawl_svc import build_list_urls
from src.page_svc import load_page_records
from src.url_guard import check_url, UrlRejected, policy, allowed_ports, demo_hosts
from src.rate_limit import check as rate_check, RateLimited, stats as rate_stats

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
STATIC_DIR = os.path.join(BASE_DIR, "src", "static")

app = FastAPI(
    title="arXiv 采集分析智能体",
    description="Crawl4AI 采集 + 确定性清洗 + DuckDB 存储 + LIDA 分析出图",
    version="1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# 公网部署时开启（.env 里 PUBLIC_AUTH=1），本地开发不设即可免登录
from src.auth import BasicAuthMiddleware  # noqa: E402

app.add_middleware(BasicAuthMiddleware)


# ---------------------------------------------------------------- 请求模型
class CollectReq(BaseModel):
    category: str = Field(default="cs.AI", description="arXiv 分类，如 cs.AI、cs.LG")
    max_entries: int = Field(default=100, ge=10, le=2000, description="采样条数")


class AnalyzeReq(BaseModel):
    category: str = Field(default="cs.AI")
    goal: str = Field(default="按团队规模统计论文数量（1人/2-4人/5-8人/9人以上）",
                      description="中文分析目标，会作为图表标题，建议 20 字以内")


class PageReq(BaseModel):
    url: str = Field(description="任意公开网页地址，例如 https://books.toscrape.com/")
    goal: str = Field(default="", description="分析目标，留空则由智能体根据数据结构自动决定")


class WatchReq(BaseModel):
    url: str = Field(description="要持续监控的网址")
    goal: str = Field(default="", description="分析目标，留空则每次自动决定")
    minutes: int = Field(default=30, ge=1, le=1440, description="检查间隔（分钟）")


# ---------------------------------------------------------------- 定时监控调度器
# 说明：这里用 APScheduler 跑在 FastAPI 进程内，原因是"检查一个网页"是轻量动作；
# 真正重的采集任务仍然走 Celery，这里只是到点往队列里丢一单。省一个进程。
scheduler = BackgroundScheduler(timezone="Asia/Shanghai")
_scheduler_on = False


def _watch_job(watch_id: str):
    """到点执行：抓一次该网址，结果照常入 pages 表（前端历史里能看到每次快照）"""
    from src.page_svc import run_page

    w = get_watch(watch_id)
    if not w or w["status"] != "active":
        return
    try:
        r = asyncio.run(run_page(w["url"], w["goal"] or None))
        touch_watch(watch_id, last_page_id=r.get("page_id"),
                    status="active" if r.get("ok") else "active")
    except Exception:
        touch_watch(watch_id)


def _schedule_watch(watch_id: str, minutes: int, first_delay_sec: int = 60):
    from datetime import timedelta

    scheduler.add_job(
        _watch_job, "interval", minutes=int(minutes), id=watch_id, args=[watch_id],
        replace_existing=True, max_instances=1, coalesce=True,
        next_run_time=datetime.now() + timedelta(seconds=first_delay_sec),
    )


# ---------------------------------------------------------------- 启动
@app.on_event("startup")
def _startup():
    global _scheduler_on
    init_db()
    if not _scheduler_on:
        scheduler.start()
        _scheduler_on = True
    # 重启后把库里 active 的监控重新挂上（进程重启不丢配置）
    for w in list_watches():
        if w["watch_id"] not in {j.id for j in scheduler.get_jobs()}:
            _schedule_watch(w["watch_id"], w["minutes"])


@app.on_event("shutdown")
def _shutdown():
    global _scheduler_on
    if _scheduler_on:
        scheduler.shutdown(wait=False)
        _scheduler_on = False


@app.get("/api/health")
def health():
    return {"ok": True, "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}


@app.get("/api/security")
def security_info():
    """安全策略与限流现状：前端页脚展示，也方便自查配置有没有生效"""
    return {
        "policy": policy(),
        "allowed_ports": sorted(allowed_ports()),
        "demo_hosts": sorted(demo_hosts()) if policy() == "demo" else [],
        "limits": rate_stats(),
    }


# ---------------------------------------------------------------- 数据查询
@app.get("/api/stats")
def stats():
    """首页概览：总量、分类分布、月份分布、开源比例"""
    df = load_papers()
    if df.empty:
        return {"total": 0, "by_category": [], "by_month": [], "has_code_ratio": 0,
                "avg_author": 0, "agent_title_ratio": 0}

    by_cat = df["category"].value_counts().to_dict()
    by_month = df["month"].value_counts().sort_index().tail(12).to_dict()
    return {
        "total": int(len(df)),
        "by_category": [{"name": k, "value": int(v)} for k, v in by_cat.items()],
        "by_month": [{"name": k, "value": int(v)} for k, v in by_month.items()],
        "has_code_ratio": round(float(df["has_code"].mean()), 4),
        "avg_author": round(float(df["author_count"].mean()), 2),
        "agent_title_ratio": round(float(df["title"].str.contains("agent", case=False).mean()), 4),
    }


@app.get("/api/papers")
def papers(category: str = None, limit: int = 20):
    """最新入库的论文，前端表格预览用"""
    df = load_papers(category=category, limit=int(limit))
    if df.empty:
        return {"total": 0, "rows": []}
    cols = ["paper_id", "title", "first_author", "author_count",
            "primary_subject", "month", "announce_date", "has_code"]
    return {
        "total": int(count_papers(category)),
        "rows": json.loads(df[cols].to_json(orient="records", force_ascii=False)),
    }


@app.get("/api/tasks")
def tasks(limit: int = 10):
    df = get_recent_tasks(limit)
    if df.empty:
        return {"rows": []}
    return {"rows": json.loads(df.to_json(orient="records", force_ascii=False))}


@app.get("/api/wash_report/{task_id}")
def wash_report(task_id: str):
    """单次任务的清洗报告（存在 analyses/tasks 之外，从 markdown 旁边的 json 读不到时返回空）"""
    con = get_con(read_only=True)
    try:
        rows = con.execute(
            "SELECT task_id, category, status, rows_kept, new_rows, raw_md_path, error, updated_at "
            "FROM tasks WHERE task_id = ?", [task_id]
        ).fetchdf()
    finally:
        con.close()
    if rows.empty:
        raise HTTPException(404, "任务不存在")
    return json.loads(rows.to_json(orient="records", force_ascii=False))[0]


# ---------------------------------------------------------------- 提交任务
@app.post("/api/collect")
def collect(req: CollectReq):
    """异步采集（只入库，不花钱调大模型）"""
    try:
        r = collect_task.delay(req.category, req.max_entries)
    except Exception as e:
        raise HTTPException(503, f"任务队列不可用：{type(e).__name__}: {e}")
    return {"task_id": r.id, "kind": "collect", "category": req.category}


@app.post("/api/analyze")
def analyze(req: AnalyzeReq):
    """异步分析（调 LIDA + 大模型，成本高）"""
    if count_papers(req.category) == 0:
        raise HTTPException(400, "该分类还没有数据，请先采集")
    try:
        r = analyze_task.delay(req.category, req.goal, "api")
    except Exception as e:
        raise HTTPException(503, f"任务队列不可用：{type(e).__name__}: {e}")
    return {"task_id": r.id, "kind": "analyze", "category": req.category}


@app.post("/api/pipeline")
def pipeline(req: CollectReq):
    """采集 + 需要时自动分析，一步完成"""
    try:
        r = pipeline_task.delay(req.category, req.max_entries)
    except Exception as e:
        raise HTTPException(503, f"任务队列不可用：{type(e).__name__}: {e}")
    return {"task_id": r.id, "kind": "pipeline", "category": req.category}


# ---------------------------------------------------------------- 通用网址流程
@app.post("/api/page")
def page_pipeline(req: PageReq, request: Request):
    """提交任意网址：抓取 → 自动抽结构 → 摘要 → 出图（异步）

    顺序：先做 URL 安全准入（便宜且能给出人话原因），再走限流（通过校验才占额度），
    最后才丢进队列。这样"被安全拦下的请求"不会吃掉用户的提交次数。
    """
    client_ip = (request.client.host if request.client else "unknown") or "unknown"
    try:
        url = check_url(req.url)
    except UrlRejected as e:
        raise HTTPException(400, detail=e.reason)

    host = urlparse(url).hostname or ""
    try:
        rate_check(client_ip, host)
    except RateLimited as e:
        raise HTTPException(429, detail=e.reason, headers={"Retry-After": str(e.retry_after)})

    try:
        r = page_task.delay(url, req.goal or None)
    except Exception as e:
        raise HTTPException(503, f"任务队列不可用：{type(e).__name__}: {e}")
    return {"task_id": r.id, "kind": "page", "url": url}


@app.get("/api/page/{page_id}")
def page_detail(page_id: str, records_limit: int = 20):
    """某个页面的解析结果：元信息 + 摘要 + 分析目标 + 图表 + 数据预览"""
    p = get_page(page_id)
    if not p:
        raise HTTPException(404, "页面不存在")
    from src.page_svc import public_path

    return {
        "page_id": p["page_id"],
        "url": p["url"],
        "title": p["title"],
        "adapter": p["adapter"],
        "fetched_at": p["fetched_at"],
        "table_count": p["table_count"],
        "row_count": p["row_count"],
        "col_count": p["col_count"],
        "columns": p.get("columns") or [],
        "summary": p.get("summary") or "",
        "goals": p.get("goals") or [],
        "charts": [{**c, "image_path": public_path(c.get("image_path", ""))}
                   for c in (p.get("charts") or [])],
        "status": p["status"],
        "error": p["error"],
        "records": load_page_records(page_id, records_limit),
    }


@app.get("/api/pages")
def pages_list(limit: int = 15):
    df = list_pages(limit)
    if df.empty:
        return {"total": 0, "rows": []}
    return {"total": count_pages(),
            "rows": json.loads(df.to_json(orient="records", force_ascii=False))}


# ---------------------------------------------------------------- 定时监控
@app.post("/api/watch")
def create_watch(req: WatchReq):
    """给某个网址开定时监控：到点自动抓一次、自动出摘要和图表，结果进历史记录"""
    try:
        url = check_url(req.url)
    except UrlRejected as e:
        raise HTTPException(400, detail=e.reason)
    watch_id = "w_" + hashlib.sha1(f"{url}|{req.goal}".encode("utf-8")).hexdigest()[:10]
    save_watch(watch_id, url, req.goal or "", req.minutes)
    _schedule_watch(watch_id, req.minutes)
    job = scheduler.get_job(watch_id)
    return {"watch_id": watch_id, "url": url, "minutes": req.minutes,
            "next_run": job.next_run_time.strftime("%Y-%m-%d %H:%M:%S") if job and job.next_run_time else None}


@app.get("/api/watches")
def watches():
    rows = list_watches()
    for r in rows:
        job = scheduler.get_job(r["watch_id"])
        r["next_run"] = job.next_run_time.strftime("%Y-%m-%d %H:%M:%S") if job and job.next_run_time else None
    return {"total": len(rows), "rows": rows}


@app.delete("/api/watch/{watch_id}")
def stop_watch(watch_id: str):
    if scheduler.get_job(watch_id):
        scheduler.remove_job(watch_id)
    delete_watch(watch_id)
    return {"watch_id": watch_id, "status": "stopped"}


@app.post("/api/watch/{watch_id}/run")
def run_watch_now(watch_id: str):
    """立刻执行一次（演示用，不用等间隔到点）"""
    w = get_watch(watch_id)
    if not w:
        raise HTTPException(404, "监控不存在")
    scheduler.add_job(_watch_job, "date", run_date=datetime.now(), args=[watch_id],
                      id=f"{watch_id}_now", replace_existing=True)
    return {"watch_id": watch_id, "status": "triggered"}


@app.get("/api/task/{task_id}")
def task_state(task_id: str):
    """查任务状态（SSE 不可用时的兜底轮询接口）"""
    r = AsyncResult(task_id, app=celery_app)
    payload = {"task_id": task_id, "state": r.state}
    if r.state == "PROGRESS":
        payload["meta"] = r.info
    elif r.state == "SUCCESS":
        payload["result"] = r.result
    elif r.state == "FAILURE":
        payload["error"] = str(r.info)
    return payload


@app.get("/api/stream/{task_id}")
async def stream(task_id: str, timeout_sec: int = 900):
    """SSE 实时推送任务状态"""
    async def gen():
        r = AsyncResult(task_id, app=celery_app)
        start = asyncio.get_event_loop().time()
        last = None
        yield f"data: {json.dumps({'state': r.state, 'task_id': task_id}, ensure_ascii=False)}\n\n"
        while True:
            await asyncio.sleep(1.5)
            state = r.state
            if state != last:
                body = {"state": state, "task_id": task_id}
                if state == "PROGRESS":
                    body["meta"] = r.info if isinstance(r.info, dict) else str(r.info)
                elif state == "SUCCESS":
                    body["result"] = r.result
                elif state == "FAILURE":
                    body["error"] = str(r.info)
                yield f"data: {json.dumps(body, ensure_ascii=False, default=str)}\n\n"
                last = state
            if r.ready():
                break
            if asyncio.get_event_loop().time() - start > timeout_sec:
                yield f"data: {json.dumps({'state': 'TIMEOUT', 'task_id': task_id})}\n\n"
                break

    return StreamingResponse(gen(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


# ---------------------------------------------------------------- 图表
@app.get("/api/charts")
def list_charts(limit: int = 30):
    """列出已生成的图表，按修改时间倒序"""
    if not os.path.isdir(CHART_DIR):
        return {"rows": []}
    files = [f for f in os.listdir(CHART_DIR) if f.lower().endswith(".png")]
    files.sort(key=lambda f: os.path.getmtime(os.path.join(CHART_DIR, f)), reverse=True)
    rows = []
    for f in files[:limit]:
        p = os.path.join(CHART_DIR, f)
        rows.append({
            "name": f,
            "url": f"/charts/{f}",
            "size_kb": round(os.path.getsize(p) / 1024, 1),
            "mtime": datetime.fromtimestamp(os.path.getmtime(p)).strftime("%Y-%m-%d %H:%M:%S"),
        })
    return {"rows": rows}


@app.get("/charts/{filename}")
def get_chart(filename: str):
    if "/" in filename or "\\" in filename:
        raise HTTPException(400, "非法文件名")
    p = os.path.join(CHART_DIR, filename)
    if not os.path.isfile(p):
        raise HTTPException(404, "图表不存在")
    return FileResponse(p, media_type="image/png")


# ---------------------------------------------------------------- 首页
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/")
def index():
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))
