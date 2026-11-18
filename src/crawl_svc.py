"""arXiv 采集与清洗（Crawl4AI 采集 + 确定性规则清洗）

分工原则（面试重点）：
- Crawl4AI 只负责“把网页拿回来并去掉噪声”，产出 cleaned_html
- 字段解析、类型转换、去重、异常标记全部由确定性规则完成，不依赖大模型
- 大模型只在后面的 LIDA 分析阶段参与，避免幻觉污染事实数据

清洗规则（R1~R7）：
R1 文本规范化：HTML 实体反转义、去零宽字符、压缩连续空白、去首尾空格、标题去句末句点
R2 主键修正：paper_id 去版本号（v1/v2/v3），用正则校验 4位年+月.4~5位序号，不合规整行丢弃
R3 关键字段缺失：title 或 authors 为空 → 整行丢弃（这两列是分析的基础，缺失无法补齐）
R4 去重：同一 paper_id 只保留一条（保留公告日期最新的一条）
R5 类型转换：author_count 转整数；month 由 paper_id 派生为 YYYY-MM；title_len 整数；has_code 布尔
R6 异常标记（只打标不删）：title_len < 10 或 > 250 → is_flagged=True；author_count > 30 也打标
R7 派生特征：first_author 取第一作者；has_code 判断 comments 是否提到 github/code

返回清洗报告 wash_report，记录每一条规则命中多少行，方便在页面上展示「本次采集清洗质量」。
"""
import asyncio
import os
import re
import html as html_lib
import uuid
from datetime import datetime

import pandas as pd
from bs4 import BeautifulSoup
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode

from src.db import save_raw_markdown, create_task, update_task, upsert_papers

CLEAN_VERSION = "v1"

PAPER_ID_RE = re.compile(r"\d{4}\.\d{4,5}")
VER_RE = re.compile(r"v\d+$")
ZW_RE = re.compile(r"[\u200b-\u200f\u202a-\u202e\ufeff]")
WS_RE = re.compile(r"\s+")
SUBJECT_RE = re.compile(r"\(([a-z\-]+(?:\.[A-Za-z]{2})?)\)")
DATE_RE = re.compile(r"(\d{1,2})\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{4})")

MONTHS = {
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
    "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
}


# ---------------------------------------------------------------- R1 工具函数
def normalize_text(s: str) -> str:
    """R1：反转义 + 去零宽字符 + 压缩空白 + 去首尾空格"""
    if s is None:
        return ""
    s = html_lib.unescape(str(s))
    s = ZW_RE.sub("", s)
    s = s.replace("\xa0", " ")
    s = WS_RE.sub(" ", s)
    return s.strip()


def strip_label(s: str, label: str) -> str:
    """去掉 'Title:' 'Comments:' 这类前缀"""
    s = normalize_text(s)
    if s.lower().startswith(label.lower()):
        s = s[len(label):]
    return s.strip(" :\u00a0")


# ---------------------------------------------------------------- 解析
def parse_arxiv_listing(page_html: str) -> list:
    """从 arXiv 列表页解析出论文原始记录（未清洗）"""
    soup = BeautifulSoup(page_html, "html.parser")
    dl = soup.find("dl", id="articles")
    if dl is None:
        return []

    records = []
    current_date = None
    pending_dt = None

    for el in dl.children:
        if getattr(el, "name", None) == "h3":
            text = normalize_text(el.get_text(" "))
            m = DATE_RE.search(text)
            if m:
                day, mon, year = m.group(1), m.group(2), m.group(3)
                try:
                    current_date = datetime(int(year), MONTHS[mon], int(day)).strftime("%Y-%m-%d")
                except ValueError:
                    current_date = None
        elif getattr(el, "name", None) == "dt":
            link = el.find("a", href=re.compile(r"^/abs/"))
            if link and link.get("href"):
                arxiv_id = link["href"].split("/abs/")[-1]
                pending_dt = {"raw_id": arxiv_id, "announce_date": current_date or ""}
            else:
                pending_dt = None
        elif getattr(el, "name", None) == "dd" and pending_dt:
            meta = el.find("div", class_="meta") or el

            title_div = meta.find("div", class_="list-title")
            authors_div = meta.find("div", class_="list-authors")
            comments_div = meta.find("div", class_="list-comments")
            subjects_div = meta.find("div", class_="list-subjects")

            rec = dict(pending_dt)
            rec["title"] = strip_label(title_div.get_text(" ") if title_div else "", "Title:")
            rec["authors"] = normalize_text(authors_div.get_text(" ") if authors_div else "")
            rec["comments"] = strip_label(comments_div.get_text(" ") if comments_div else "", "Comments:")
            rec["subjects"] = strip_label(subjects_div.get_text(" ") if subjects_div else "", "Subjects:")

            primary = ""
            if subjects_div:
                m = SUBJECT_RE.search(subjects_div.get_text(" "))
                if m:
                    primary = m.group(1)
            rec["primary_subject"] = primary
            records.append(rec)
            pending_dt = None

    return records


# ---------------------------------------------------------------- 采集
def build_list_urls(category: str, max_entries: int) -> list:
    """按每页 100 条拼分页 URL（arXiv 支持 show=25/50/100/200/2000）"""
    show = 100
    urls = []
    skip = 0
    while skip < max_entries:
        urls.append(f"https://arxiv.org/list/{category}/recent?skip={skip}&show={show}")
        skip += show
    return urls


async def crawl_arxiv(category: str = "cs.AI", max_entries: int = 100, max_retry: int = 3) -> tuple:
    """采集 arXiv 列表页。

    arXiv 页面偶尔响应很慢（实测单页 20~60s），所以这里做了两件事：
    1. 把 page_timeout 提到 120 秒，避免默认 60 秒就判失败
    2. 每个 URL 最多重试 3 次，失败入 failed_urls 由上层报告，不静默丢数据
    """
    urls = build_list_urls(category, max_entries)

    run_cfg = CrawlerRunConfig(
        page_timeout=120000,
        wait_until="domcontentloaded",
        cache_mode=CacheMode.BYPASS,
        verbose=False,
    )
    browser_cfg = BrowserConfig(
        headless=True,
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/159.0.0.0 Safari/537.36"
        ),
    )

    all_records = []
    all_md = []
    failed_urls = []

    async with AsyncWebCrawler(config=browser_cfg, verbose=False) as crawler:
        for url in urls:
            ok = False
            for attempt in range(1, max_retry + 1):
                if attempt > 1:
                    await asyncio.sleep(3)
                try:
                    result = await crawler.arun(url=url, config=run_cfg)
                except Exception as e:
                    print(f"[重试{attempt}] {url} 异常: {type(e).__name__}: {str(e)[:120]}")
                    continue
                if result.success:
                    page_html = result.cleaned_html or result.html or ""
                    if page_html:
                        all_md.append(f"\n\n<!-- {url} -->\n\n" + (result.markdown or ""))
                        all_records.extend(parse_arxiv_listing(page_html))
                        ok = True
                        break
                print(f"[重试{attempt}] {url} 失败: {str(result.error_message)[:120]}")
            if not ok:
                failed_urls.append(url)
            if len(all_records) >= max_entries:
                break

    raw_df = pd.DataFrame(all_records[:max_entries])
    meta = {"failed_urls": failed_urls, "fetched_pages": len(urls) - len(failed_urls)}
    return raw_df, "\n".join(all_md), meta


# ---------------------------------------------------------------- 清洗
def clean_papers(raw_df: pd.DataFrame, category: str = "") -> tuple:
    """对原始记录做确定性清洗，返回 (清洗后 DataFrame, 清洗报告 dict)"""
    report = {
        "rows_in": int(len(raw_df)),
        "dropped_invalid_id": 0,
        "dropped_missing_field": 0,
        "dropped_duplicated": 0,
        "flagged_atypical": 0,
        "rows_out": 0,
    }
    if raw_df.empty:
        return pd.DataFrame(columns=PAPER_COLUMNS_FOR_DB), report

    df = raw_df.copy()

    # R1 文本规范化
    for col in ["title", "authors", "comments", "subjects", "primary_subject"]:
        if col in df.columns:
            df[col] = df[col].map(normalize_text)
    df["title"] = df["title"].str.rstrip(".。 ")

    # R2 主键修正
    df["paper_id"] = df.apply(
        lambda r: (VER_RE.sub("", str(r.get("raw_id", ""))) if PAPER_ID_RE.search(VER_RE.sub("", str(r.get("raw_id", "")))) else None),
        axis=1,
    )
    bad_id = df["paper_id"].isna()
    report["dropped_invalid_id"] = int(bad_id.sum())
    df = df[~bad_id]

    # R3 关键字段缺失
    miss = (df["title"].str.len() < 1) | (df["authors"].str.len() < 1)
    report["dropped_missing_field"] = int(miss.sum())
    df = df[~miss]

    # R4 去重（同一 paper_id 保留公告日期最新的一条）
    df = df.sort_values("announce_date", ascending=False)
    dup = df.duplicated(subset=["paper_id"], keep="first")
    report["dropped_duplicated"] = int(dup.sum())
    df = df[~dup]

    # R5 类型转换与派生
    df["author_count"] = df["authors"].map(lambda s: len([a for a in s.split(",") if a.strip()]))
    df["first_author"] = df["authors"].map(lambda s: s.split(",")[0].strip() if s else "")
    df["title_len"] = df["title"].str.len().astype(int)
    df["month"] = df["paper_id"].map(lambda pid: f"20{pid[:2]}-{pid[2:4]}")

    # R7 派生特征
    df["has_code"] = df["comments"].str.lower().str.contains("github|code", na=False)

    # R6 异常标记（只打标不删）
    atypical = ((df["title_len"] < 10) | (df["title_len"] > 250) | (df["author_count"] > 30))
    df["is_flagged"] = atypical
    report["flagged_atypical"] = int(atypical.sum())

    df["category"] = category
    df["clean_version"] = CLEAN_VERSION
    df["ingested_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    report["rows_out"] = int(len(df))
    report["drop_rate"] = round(1 - (len(df) / report["rows_in"]), 4) if report["rows_in"] else 0.0

    return df.reset_index(drop=True), report


PAPER_COLUMNS_FOR_DB = [
    "paper_id", "title", "authors", "author_count", "first_author",
    "primary_subject", "subjects", "comments", "month", "announce_date",
    "title_len", "has_code", "is_flagged", "category", "clean_version", "ingested_at",
]


# ---------------------------------------------------------------- 对外主流程
async def run_collect(category: str = "cs.AI", max_entries: int = 100) -> dict:
    """一条龙：建任务 → 采集 → 落盘原文 → 清洗 → 入库，返回任务信息"""
    task_id = uuid.uuid4().hex[:12]
    start_url = build_list_urls(category, max_entries)[0]
    create_task(task_id, start_url, category, status="crawling")
    try:
        raw_df, raw_md, crawl_meta = await crawl_arxiv(category, max_entries)
        md_path = save_raw_markdown(task_id, raw_md)
        clean_df, report = clean_papers(raw_df, category=category)
        update_task(task_id, status="cleaning")
        total = upsert_papers(clean_df)
        update_task(task_id, status="done", raw_md_path=md_path, rows_kept=len(clean_df))
        return {
            "task_id": task_id,
            "category": category,
            "urls": build_list_urls(category, max_entries),
            "failed_urls": crawl_meta["failed_urls"],
            "fetched_pages": crawl_meta["fetched_pages"],
            "raw_md_path": md_path,
            "rows_raw": len(raw_df),
            "rows_clean": len(clean_df),
            "total_in_db": total,
            "wash_report": report,
        }
    except Exception as e:
        update_task(task_id, status="failed", error=f"{type(e).__name__}: {e}")
        raise
