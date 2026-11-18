"""DuckDB 存储层

三张表：
- papers        论文事实表（爬取并清洗后的结构化数据，主键 paper_id）
- tasks         任务表（一次采集任务的元信息、状态、错误信息）
- analyses      分析结果表（LIDA 生成的图表与分析结论，按任务归档）

设计原则：
1. 数据库只存结构化数据，原始网页 markdown 落盘到 data/raw_md，库里只存文件路径
2. 论文表用 INSERT OR REPLACE，靠 paper_id 天然去重，方便做增量更新
3. 所有时间字段统一存 ISO 字符串（YYYY-MM-DD / YYYY-MM），避免时区问题
"""
import os
import json
import uuid
from datetime import datetime

import duckdb
import pandas as pd
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(BASE_DIR, ".env"))

DB_PATH = os.getenv("DUCKDB_PATH") or os.path.join(BASE_DIR, "data", "crawl_data.duckdb")
RAW_MD_DIR = os.getenv("RAW_MD_DIR") or os.path.join(BASE_DIR, "data", "raw_md")

PAPER_COLUMNS = [
    "paper_id",
    "title",
    "authors",
    "author_count",
    "first_author",
    "primary_subject",
    "subjects",
    "comments",
    "month",
    "announce_date",
    "title_len",
    "has_code",
    "is_flagged",
    "category",
    "clean_version",
    "ingested_at",
]


def get_con(read_only: bool = False):
    """获取 DuckDB 连接（每次调用返回新连接，用完记得 close）

    注意：DuckDB 在同一进程内不允许对同一个数据库文件混用「只读」和「读写」连接，
    否则报 `Can't open a connection to same database file with a different configuration`。
    所以这里统一用读写连接，read_only 参数仅为兼容旧调用保留。
    """
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    return duckdb.connect(DB_PATH)


def init_db():
    """建表，幂等"""
    con = get_con()
    try:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS papers (
                paper_id        VARCHAR PRIMARY KEY,
                title           VARCHAR,
                authors         VARCHAR,
                author_count    INTEGER,
                first_author    VARCHAR,
                primary_subject VARCHAR,
                subjects        VARCHAR,
                comments        VARCHAR,
                month           VARCHAR,
                announce_date   VARCHAR,
                title_len       INTEGER,
                has_code        BOOLEAN,
                is_flagged      BOOLEAN,
                category        VARCHAR,
                clean_version   VARCHAR,
                ingested_at     VARCHAR
            )
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS tasks (
                task_id     VARCHAR PRIMARY KEY,
                url         VARCHAR,
                category    VARCHAR,
                status      VARCHAR,
                raw_md_path VARCHAR,
                rows_kept   INTEGER,
                created_at  VARCHAR,
                updated_at  VARCHAR,
                error       VARCHAR
            )
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS analyses (
                analysis_id VARCHAR PRIMARY KEY,
                task_id     VARCHAR,
                goal        VARCHAR,
                charts_json VARCHAR,
                created_at  VARCHAR
            )
            """
        )
        # 增量监控状态表：记录每个监控对象上一次看到的“内容指纹”，用于判断页面是否变化
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS watch_state (
                watch_key       VARCHAR PRIMARY KEY,
                category        VARCHAR,
                fingerprint     VARCHAR,
                paper_count     INTEGER,
                last_total      INTEGER,
                last_new        INTEGER,
                check_count     INTEGER,
                last_check_at   VARCHAR
            )
            """
        )
        # 通用网页解析结果表：一个 URL 一条记录
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS pages (
                page_id        VARCHAR PRIMARY KEY,
                url            VARCHAR,
                title          VARCHAR,
                adapter        VARCHAR,
                fetched_at     VARCHAR,
                markdown_path  VARCHAR,
                parquet_path   VARCHAR,
                table_count    INTEGER,
                row_count      INTEGER,
                col_count      INTEGER,
                columns_json   VARCHAR,
                summary        VARCHAR,
                goals_json     VARCHAR,
                charts_json    VARCHAR,
                status         VARCHAR,
                error          VARCHAR
            )
            """
        )
        # 定时监控的任务表：一条 URL 一条监控配置
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS page_watches (
                watch_id     VARCHAR PRIMARY KEY,
                url          VARCHAR,
                goal         VARCHAR,
                minutes      INTEGER,
                created_at   VARCHAR,
                last_run     VARCHAR,
                last_page_id VARCHAR,
                runs         INTEGER,
                status       VARCHAR
            )
            """
        )
        # 老库升级：给 tasks 表补新增列（幂等，已存在则跳过）
        for col, dtype in [("new_rows", "INTEGER"), ("fingerprint", "VARCHAR")]:
            try:
                con.execute(f"ALTER TABLE tasks ADD COLUMN {col} {dtype}")
            except Exception:
                pass
    finally:
        con.close()
    return DB_PATH


def save_raw_markdown(task_id: str, markdown: str) -> str:
    """原始 markdown 落盘，只把路径写进数据库"""
    os.makedirs(RAW_MD_DIR, exist_ok=True)
    path = os.path.join(RAW_MD_DIR, f"{task_id}.md")
    with open(path, "w", encoding="utf-8") as f:
        f.write(markdown or "")
    return path


def create_task(task_id: str, url: str, category: str, status: str = "pending"):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    con = get_con()
    try:
        con.execute(
            "INSERT OR REPLACE INTO tasks (task_id, url, category, status, raw_md_path, rows_kept, created_at, updated_at, error) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [task_id, url, category, status, None, None, now, now, None],
        )
    finally:
        con.close()


def update_task(task_id: str, status: str = None, raw_md_path: str = None,
                rows_kept: int = None, new_rows: int = None,
                fingerprint: str = None, error: str = None):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    con = get_con()
    try:
        con.execute(
            """
            UPDATE tasks SET
                status      = COALESCE(?, status),
                raw_md_path = COALESCE(?, raw_md_path),
                rows_kept   = COALESCE(?, rows_kept),
                new_rows    = COALESCE(?, new_rows),
                fingerprint = COALESCE(?, fingerprint),
                error       = COALESCE(?, error),
                updated_at  = ?
            WHERE task_id = ?
            """,
            [status, raw_md_path, rows_kept, new_rows, fingerprint, error, now, task_id],
        )
    finally:
        con.close()


def get_existing_ids(category: str = None) -> set:
    """取出库里已有的论文主键集合，用于算“这次新增了哪些”"""
    con = get_con(read_only=True)
    try:
        sql = "SELECT paper_id FROM papers"
        if category:
            return set(con.execute(sql + " WHERE category = ?", [category]).fetchdf()["paper_id"].tolist())
        return set(con.execute(sql).fetchdf()["paper_id"].tolist())
    finally:
        con.close()


def get_watch_state(watch_key: str):
    """读监控状态（内容指纹、累计检查次数），没有则返回 None"""
    con = get_con()
    try:
        rows = con.execute(
            "SELECT watch_key, category, fingerprint, paper_count, last_total, last_new, check_count, last_check_at "
            "FROM watch_state WHERE watch_key = ?", [watch_key]
        ).fetchall()
        if not rows:
            return None
        keys = ["watch_key", "category", "fingerprint", "paper_count",
                "last_total", "last_new", "check_count", "last_check_at"]
        return dict(zip(keys, rows[0]))
    finally:
        con.close()


def save_watch_state(watch_key: str, category: str, fingerprint: str,
                     paper_count: int, total: int, new_rows: int, check_count: int):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    con = get_con()
    try:
        con.execute(
            """
            INSERT OR REPLACE INTO watch_state
                (watch_key, category, fingerprint, paper_count, last_total, last_new, check_count, last_check_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [watch_key, category, fingerprint, paper_count, total, new_rows, check_count, now],
        )
    finally:
        con.close()


def get_recent_tasks(limit: int = 20) -> pd.DataFrame:
    """最近的任务列表，前端进度面板要用"""
    con = get_con(read_only=True)
    try:
        return con.execute(
            "SELECT task_id, category, status, rows_kept, new_rows, created_at, updated_at, error "
            "FROM tasks ORDER BY created_at DESC LIMIT ?", [int(limit)]
        ).fetchdf()
    finally:
        con.close()


def upsert_papers(df: pd.DataFrame) -> int:
    """把清洗后的论文表写入库；paper_id 相同则覆盖（增量更新的基础）

    用 DuckDB 的 replacement scan 直接读局部 DataFrame 变量，避免 register/unregister
    带来的“结果集未消费”报错。
    """
    if df is None or df.empty:
        return count_papers()
    missing = [c for c in PAPER_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"缺少字段: {missing}")

    dfw = df[PAPER_COLUMNS].copy()
    cols = ", ".join(PAPER_COLUMNS)
    con = get_con()
    try:
        con.execute(
            f"INSERT OR REPLACE INTO papers ({cols}) SELECT {cols} FROM dfw"
        ).fetchall()
        return con.execute("SELECT COUNT(*) FROM papers").fetchone()[0]
    finally:
        con.close()


def load_papers(category: str = None, month_from: str = None, limit: int = None) -> pd.DataFrame:
    """从库里读论文表成 DataFrame，供分析智能体使用"""
    con = get_con(read_only=False)
    try:
        sql = "SELECT * FROM papers"
        conds, params = [], []
        if category:
            conds.append("category = ?")
            params.append(category)
        if month_from:
            conds.append("month >= ?")
            params.append(month_from)
        if conds:
            sql += " WHERE " + " AND ".join(conds)
        sql += " ORDER BY announce_date DESC, paper_id DESC"
        if limit:
            sql += f" LIMIT {int(limit)}"
        return con.execute(sql, params).fetchdf()
    finally:
        con.close()


def count_papers(category: str = None) -> int:
    con = get_con()
    try:
        if category:
            return con.execute("SELECT COUNT(*) FROM papers WHERE category = ?", [category]).fetchone()[0]
        return con.execute("SELECT COUNT(*) FROM papers").fetchone()[0]
    finally:
        con.close()


def save_analysis(task_id: str, goal: str, charts: list) -> str:
    """分析结果入库，图表以 JSON 形式保存（图片路径也在里面）"""
    analysis_id = uuid.uuid4().hex[:12]
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    con = get_con()
    try:
        con.execute(
            "INSERT INTO analyses (analysis_id, task_id, goal, charts_json, created_at) VALUES (?, ?, ?, ?, ?)",
            [analysis_id, task_id, goal, json.dumps(charts, ensure_ascii=False, default=str), now],
        )
    finally:
        con.close()
    return analysis_id


# ---------------------------------------------------------------- 通用网页（pages 表）
def save_page(page_id: str, url: str, title: str, adapter: str, markdown_path: str,
              parquet_path: str, table_count: int, row_count: int, col_count: int,
              columns: list, status: str = "extracted"):
    con = get_con()
    try:
        con.execute(
            """
            INSERT OR REPLACE INTO pages
              (page_id, url, title, adapter, fetched_at, markdown_path, parquet_path,
               table_count, row_count, col_count, columns_json, summary, goals_json,
               charts_json, status, error)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, ?, NULL)
            """,
            [page_id, url, title, adapter, datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
             markdown_path, parquet_path, int(table_count), int(row_count), int(col_count),
             json.dumps(columns, ensure_ascii=False), status],
        )
    finally:
        con.close()


def update_page(page_id: str, summary: str = None, goals: list = None,
                charts: list = None, status: str = None, error: str = None):
    con = get_con()
    try:
        con.execute(
            """
            UPDATE pages SET
                summary     = COALESCE(?, summary),
                goals_json  = COALESCE(?, goals_json),
                charts_json = COALESCE(?, charts_json),
                status      = COALESCE(?, status),
                error       = COALESCE(?, error)
            WHERE page_id = ?
            """,
            [summary,
             json.dumps(goals, ensure_ascii=False) if goals is not None else None,
             json.dumps(charts, ensure_ascii=False, default=str) if charts is not None else None,
             status, error, page_id],
        )
    finally:
        con.close()


def get_page(page_id: str) -> dict:
    con = get_con()
    try:
        rows = con.execute(
            "SELECT page_id, url, title, adapter, fetched_at, markdown_path, parquet_path, "
            "table_count, row_count, col_count, columns_json, summary, goals_json, charts_json, status, error "
            "FROM pages WHERE page_id = ?", [page_id]
        ).fetchall()
    finally:
        con.close()
    if not rows:
        return None
    keys = ["page_id", "url", "title", "adapter", "fetched_at", "markdown_path", "parquet_path",
            "table_count", "row_count", "col_count", "columns_json", "summary", "goals_json",
            "charts_json", "status", "error"]
    d = dict(zip(keys, rows[0]))
    for k in ["columns_json", "goals_json", "charts_json"]:
        try:
            d[k.replace("_json", "")] = json.loads(d[k]) if d[k] else None
        except Exception:
            d[k.replace("_json", "")] = None
    return d


def list_pages(limit: int = 20) -> pd.DataFrame:
    """历史列表：同一个 URL 只显示最近一次快照，并带出累计快照数
    （定时监控会反复抓同一网址，不去重的话列表会被同一条目刷屏）
    """
    con = get_con()
    try:
        return con.execute(
            """
            SELECT p.page_id, p.url, p.title, p.adapter, p.row_count, p.status, p.fetched_at,
                   (SELECT COUNT(*) FROM pages q WHERE q.url = p.url) AS snapshots
            FROM pages p
            WHERE p.fetched_at = (SELECT MAX(fetched_at) FROM pages q2 WHERE q2.url = p.url)
            ORDER BY p.fetched_at DESC
            LIMIT ?
            """, [int(limit)]
        ).fetchdf()
    finally:
        con.close()


def count_pages() -> int:
    con = get_con()
    try:
        return con.execute("SELECT COUNT(*) FROM pages").fetchone()[0]
    finally:
        con.close()


# ---------------------------------------------------------------- 定时监控（page_watches 表）
def save_watch(watch_id: str, url: str, goal: str, minutes: int):
    con = get_con()
    try:
        con.execute(
            "INSERT OR REPLACE INTO page_watches "
            "(watch_id, url, goal, minutes, created_at, last_run, last_page_id, runs, status) "
            "VALUES (?, ?, ?, ?, ?, NULL, NULL, 0, 'active')",
            [watch_id, url, goal, int(minutes),
             datetime.now().strftime("%Y-%m-%d %H:%M:%S")],
        )
    finally:
        con.close()


def list_watches() -> list:
    con = get_con()
    try:
        rows = con.execute(
            "SELECT watch_id, url, goal, minutes, created_at, last_run, last_page_id, runs, status "
            "FROM page_watches WHERE status = 'active' ORDER BY created_at DESC"
        ).fetchall()
    finally:
        con.close()
    keys = ["watch_id", "url", "goal", "minutes", "created_at", "last_run", "last_page_id", "runs", "status"]
    return [dict(zip(keys, r)) for r in rows]


def get_watch(watch_id: str) -> dict:
    con = get_con()
    try:
        rows = con.execute(
            "SELECT watch_id, url, goal, minutes, created_at, last_run, last_page_id, runs, status "
            "FROM page_watches WHERE watch_id = ?", [watch_id]
        ).fetchall()
    finally:
        con.close()
    if not rows:
        return None
    keys = ["watch_id", "url", "goal", "minutes", "created_at", "last_run", "last_page_id", "runs", "status"]
    return dict(zip(keys, rows[0]))


def touch_watch(watch_id: str, last_page_id: str = None, status: str = None, inc_runs: bool = True):
    con = get_con()
    try:
        con.execute(
            "UPDATE page_watches SET last_run = ?, last_page_id = COALESCE(?, last_page_id), "
            "runs = runs + ?, status = COALESCE(?, status) WHERE watch_id = ?",
            [datetime.now().strftime("%Y-%m-%d %H:%M:%S"), last_page_id,
             1 if inc_runs else 0, status, watch_id],
        )
    finally:
        con.close()


def delete_watch(watch_id: str):
    con = get_con()
    try:
        con.execute("UPDATE page_watches SET status = 'stopped' WHERE watch_id = ?", [watch_id])
    finally:
        con.close()


if __name__ == "__main__":
    print("数据库文件:", init_db())
    print("当前论文条数:", count_papers())
