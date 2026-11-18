"""通用网页采集与解析层：输入任意公开网址，自动抽取可分析的结构

三级抽取策略（从最强到兜底，先用能用的那一级）：
1. **表格优先**：页面里有 `<table>` 且有 ≥2 行 ≥2 列 → 直接把表格变成 DataFrame
2. **重复块抽取**：同一种签名的元素在页面重复出现 ≥8 次（商品卡片、文章列表、论坛帖子）
   → 抽取每块的标题、链接、价格、评分、文本长度，变成"每条记录一行"的表
3. **正文分块**：以上都没有 → 按 Markdown 标题切分正文，得到"章节/字数/行数/词数"表，
   供文本统计和图表使用

另外保留 **arXiv 适配器**：命中 arxiv.org/list 时走专门的字段解析，得到更规整的论文表。
这就是"适配器分层"：对外都是 DataFrame，内部解析规则可替换。
"""
import io
import json
import os
import re
import uuid
from datetime import datetime
from urllib.parse import urljoin, urlparse

import pandas as pd
from bs4 import BeautifulSoup
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode

from src.db import BASE_DIR, save_page, update_page
from src.llm_svc import summarize_page, propose_goals

MD_DIR = os.path.join(BASE_DIR, "data", "pages")
EXT_DIR = os.path.join(BASE_DIR, "data", "extracted")
CHART_DIR = os.path.join(BASE_DIR, "data", "charts")


def public_path(p: str) -> str:
    """对外响应只给相对路径：公网接口不该泄露服务器目录结构（D:\\... 或 /app/...）"""
    if not p:
        return ""
    try:
        return os.path.relpath(p, BASE_DIR).replace("\\", "/")
    except Exception:
        return os.path.basename(p)

MONEY_RE = re.compile(r"(?:[¥$€£]|RMB|USD)\s?\d+(?:[.,]\d+)|\d+(?:\.\d+)?\s?(?:元|美元|英镑)")
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/159.0.0.0 Safari/537.36")


# ---------------------------------------------------------------- 抓取
def _try_fetch_json(url: str):
    """先探一下是不是 JSON 接口（微博热搜这类 /ajax/ 接口比浏览器抓页面更可靠）"""
    import requests

    try:
        parts = urlparse(url)
        headers = {"User-Agent": UA, "Accept": "application/json,*/*"}
        if parts.scheme and parts.netloc:
            # 很多站点的 /ajax/ 接口会校验 Referer，用同站点首页当来源是常规做法
            headers["Referer"] = f"{parts.scheme}://{parts.netloc}/"
        r = requests.get(url, headers=headers, timeout=20)
        ct = (r.headers.get("content-type") or "").lower()
        if r.status_code == 200 and "json" in ct:
            return r.json()
    except Exception:
        return None
    return None


async def crawl_any(url: str, max_retry: int = 3) -> dict:
    """抓任意网页，返回 html / markdown / title；JSON 接口则返回解析后的 json"""
    payload = _try_fetch_json(url)
    if payload is not None:
        return {"ok": True, "html": "", "markdown": json.dumps(payload, ensure_ascii=False)[:200000],
                "title": f"JSON 接口 {url}", "json": payload, "error": None}

    run_cfg = CrawlerRunConfig(page_timeout=120000, wait_until="domcontentloaded",
                              cache_mode=CacheMode.BYPASS, verbose=False)
    browser_cfg = BrowserConfig(headless=True, user_agent=UA)

    last_err = None
    async with AsyncWebCrawler(config=browser_cfg, verbose=False) as crawler:
        for attempt in range(1, max_retry + 1):
            try:
                r = await crawler.arun(url=url, config=run_cfg)
                if r.success and (r.html or r.cleaned_html):
                    # 跟随重定向后要对落地地址再校验一次，否则公网短链 302 到内网就能绕过入口校验
                    final_url = getattr(r, "redirected_url", "") or ""
                    if final_url and final_url.rstrip("/") != url.rstrip("/"):
                        try:
                            from src.url_guard import check_url

                            check_url(final_url)
                        except Exception as e:
                            return {"ok": False, "html": "", "markdown": "", "title": "",
                                    "error": f"重定向目标被拒绝：{getattr(e, 'reason', e)}"}
                    title = ""
                    try:
                        title = (r.metadata or {}).get("title") or ""
                    except Exception:
                        title = ""
                    return {
                        "ok": True,
                        "html": r.html or "",
                        "markdown": r.markdown or "",
                        "title": title.strip(),
                        "error": None,
                    }
                last_err = str(r.error_message)[:200]
            except Exception as e:
                last_err = f"{type(e).__name__}: {e}"[:200]
            if attempt < max_retry:
                import asyncio
                await asyncio.sleep(3)
    return {"ok": False, "html": "", "markdown": "", "title": "", "error": last_err}


# ---------------------------------------------------------------- 解析：JSON 接口
JSON_KEY_CN = {
    "word": "话题", "note": "话题说明", "num": "热度", "rank": "排名",
    "realpos": "原始位次", "label_name": "标签", "category": "分类",
    "name": "名称", "value": "数值", "price": "价格", "title": "标题",
}


def json_to_df(payload) -> pd.DataFrame:
    """把接口返回的 JSON 变成一张表：找到最长的一组同构对象（列表里全是 dict）"""
    best = []

    def walk(node, depth=0):
        nonlocal best
        if depth > 6:
            return
        if isinstance(node, list) and node and isinstance(node[0], dict):
            if len(node) > len(best):
                best = node
        if isinstance(node, dict):
            for v in node.values():
                walk(v, depth + 1)
        elif isinstance(node, list):
            for v in node[:30]:
                walk(v, depth + 1)

    walk(payload)
    if len(best) < 3:
        return None

    df = pd.DataFrame(best)
    # 只留扁平、有信息量的字段：URL/颜色/单一值这类杂项直接丢
    cols = []
    for c in df.columns:
        s = df[c].dropna()
        if len(s) == 0:
            continue
        if isinstance(s.iloc[0], (dict, list)):
            continue
        vals = s.astype(str)
        if (vals.str.match(r"^https?://")).mean() > 0.5:
            continue
        if (vals.str.match(r"^#[0-9a-fA-F]{3,8}$")).mean() > 0.5:
            continue
        if df[c].nunique(dropna=True) <= 1:
            continue
        cols.append(c)

    # 数值列排前面（热度、排名这些才是能画图的）；0/1 这类只有两档的字段信息量低，丢掉
    def is_numeric(col):
        conv = pd.to_numeric(df[col], errors="coerce")
        return conv.notna().mean() >= 0.6

    nums, texts = [], []
    useful_names = {"full_name", "name", "title", "word", "description", "language", "category", "tag"}
    for c in cols:
        if is_numeric(c):
            if df[c].nunique(dropna=True) > 2:
                nums.append(c)
        else:
            nu = df[c].nunique(dropna=True)
            if c in useful_names:
                texts.insert(0, c)        # 名称/描述类最有用，放最前
            elif 2 <= nu <= 40:
                texts.append(c)           # 分类字段（语言、标签）适合做分组统计
            else:
                texts.append(c)
    cols = nums[:6] + texts
    df = df[cols[:10]].copy()
    df = df.rename(columns={c: JSON_KEY_CN.get(c, c) for c in df.columns})
    for c in df.columns:
        if df[c].dtype == object:
            conv = pd.to_numeric(df[c], errors="coerce")
            if conv.notna().sum() >= max(3, int(len(df) * 0.6)):
                df[c] = conv
    return df if len(df) >= 3 else None


# ---------------------------------------------------------------- 解析：表格
def extract_tables(soup: BeautifulSoup) -> list:
    tables = []
    for tb in soup.find_all("table"):
        try:
            rows = tb.find_all("tr")
            if len(rows) < 2:
                continue
            grid = []
            for tr in rows:
                cells = tr.find_all(["th", "td"])
                vals = [c.get_text(" ", strip=True) for c in cells]
                if any(v for v in vals):
                    grid.append(vals)
            if len(grid) < 2:
                continue
            width = max(len(r) for r in grid)
            grid = [r + [""] * (width - len(r)) for r in grid]
            header = grid[0]
            # 表头全空或重复度太低，就用序号当列名
            if not any(header) or len(set(header)) < width * 0.6:
                header = [f"列{i+1}" for i in range(width)]
                body = grid
            else:
                body = grid[1:]
            if len(body) < 2 or width < 2:
                continue
            df = pd.DataFrame(body, columns=header)
            df = df.loc[:, [c for c in df.columns if c and str(c).strip()]]
            tables.append(df.reset_index(drop=True))
        except Exception:
            continue
    tables.sort(key=lambda d: d.shape[0] * d.shape[1], reverse=True)
    return tables


# ---------------------------------------------------------------- 解析：重复块
def _signature(el):
    cls = el.get("class") or []
    if not cls:
        return None
    return (el.name, tuple(sorted(cls)))


def extract_repeated_items(soup: BeautifulSoup, base_url: str) -> pd.DataFrame:
    """找页面上重复出现的同构元素块（商品卡片 / 文章列表 / 帖子行），抽成一张表

    电商站的坑：价格经常被 <span class="price">¥56.60</span> 拆开，或者写成 "¥ 56.60"，
    所以不能只靠整块文本的一条正则——先找 class/id 里带 price 的元素，再用正则兜底。
    """
    groups = {}
    for el in soup.find_all(True):
        sig = _signature(el)
        if not sig:
            continue
        groups.setdefault(sig, []).append(el)

    candidates = []
    for sig, els in groups.items():
        if len(els) < 8:
            continue
        texts = [e.get_text(" ", strip=True) for e in els]
        lens = [len(t) for t in texts]
        if not lens:
            continue
        avg = sum(lens) / len(lens)
        if avg < 10 or avg > 600:
            continue
        # 抽样算三项比例：像标题的、带价格的、带图片的（商品卡片的三个特征）
        probe = els[:12]
        titled = sum(1 for e in probe if (t := _pick_title(e)) and len(t) >= 4)
        with_price = sum(1 for e in probe if _pick_price(e)[0])
        with_img = sum(1 for e in probe if e.find("img"))
        n = max(1, len(probe))
        title_ratio, price_ratio, img_ratio = titled / n, with_price / n, with_img / n
        if title_ratio < 0.6:
            continue
        score = len(els) * (0.45 * title_ratio + 0.45 * price_ratio + 0.10 * img_ratio)
        candidates.append((score, sig, els, price_ratio))

    if not candidates:
        return None

    candidates.sort(key=lambda x: x[0], reverse=True)
    # 有价格覆盖更高的组就优先它（电商列表页 vs 导航菜单的关键区别）
    priced = [c for c in candidates if c[3] >= 0.4]
    _, sig, els, price_ratio = priced[0] if priced else candidates[0]

    rows = []
    for el in els:
        title = _pick_title(el)
        if not title:
            continue
        link = el.find("a", href=True)
        href = urljoin(base_url, link["href"]) if link else ""
        text = el.get_text(" ", strip=True)
        price, currency = _pick_price(el)
        rows.append({
            "条目标题": title,
            "标题字数": len(title),
            "价格": price,
            "币种": currency,
            "评分": _pick_rating(el),
            "评论数": _pick_comments(el),
            "条目长度": len(text),
            "链接": href[:200],
        })
    if len(rows) < 5:
        return None
    return pd.DataFrame(rows)


PRICE_CLASS_RE = re.compile(r"price|money|amount|cost|售价|价格|现价", re.I)
COMMENT_CLASS_RE = re.compile(r"comment|review|评价|评论|sales|sold|销量", re.I)
CURRENCY_MAP = {"¥": "CNY", "￥": "CNY", "$": "USD", "€": "EUR", "£": "GBP"}


def _pick_price(el):
    """返回 (数值, 币种)。先找 class/id 带 price 的元素，再退回整块文本的正则"""
    for child in el.find_all(True, limit=80):
        cls = (" ".join(child.get("class") or [])) + " " + (child.get("id") or "")
        if PRICE_CLASS_RE.search(cls):
            txt = child.get_text(" ", strip=True)
            if not txt:
                continue
            m = MONEY_RE.search(txt)
            if m:
                return _to_price(m.group(0))
    m = MONEY_RE.search(el.get_text(" ", strip=True))
    if m:
        return _to_price(m.group(0))
    return None, ""


def _to_price(token: str):
    sym = next((s for s in CURRENCY_MAP if s in token), "")
    m = re.search(r"\d+(?:[.,]\d+)?", token)
    if not m:
        return None, CURRENCY_MAP.get(sym, "")
    num = float(m.group(0).replace(",", ""))
    return num, CURRENCY_MAP.get(sym, "")


def _pick_comments(el):
    for child in el.find_all(True, limit=80):
        cls = (" ".join(child.get("class") or [])) + " " + (child.get("id") or "")
        if COMMENT_CLASS_RE.search(cls):
            m = re.search(r"(\d+(?:[.,]\d+)?)", child.get_text(" ", strip=True).replace(",", ""))
            if m:
                return int(float(m.group(1)))
    return None


def _pick_title(el):
    for tag in ["h1", "h2", "h3", "h4", "h5"]:
        t = el.find(tag)
        if t and t.get_text(strip=True):
            return t.get_text(" ", strip=True)[:200]
    a = el.find("a")
    if a and a.get_text(strip=True) and len(a.get_text(strip=True)) <= 200:
        return a.get_text(" ", strip=True)
    direct = [c.get_text(" ", strip=True) for c in el.find_all(recursive=False)]
    direct = [d for d in direct if d]
    if direct:
        return max(direct, key=len)[:200]
    return None


def _pick_rating(el):
    for child in el.find_all(True):
        cls = " ".join(child.get("class") or [])
        if "star" in cls.lower() or "rating" in cls.lower():
            m = re.search(r"(\d+(?:\.\d+)?)", cls)
            if m:
                return float(m.group(1))
    return None


# ---------------------------------------------------------------- 解析：正文分块
def extract_text_blocks(markdown: str) -> pd.DataFrame:
    lines = (markdown or "").splitlines()
    blocks, cur_head, cur_body = [], "（无标题）", []
    for ln in lines:
        if ln.strip().startswith("#"):
            if cur_body:
                blocks.append((cur_head, "\n".join(cur_body)))
            cur_head = ln.strip().lstrip("#").strip() or "（无标题）"
            cur_body = []
        else:
            cur_body.append(ln)
    if cur_body:
        blocks.append((cur_head, "\n".join(cur_body)))

    rows = []
    for head, body in blocks:
        body = body.strip()
        if len(body) < 40:
            continue
        rows.append({
            "章节": head[:80],
            "字数": len(body),
            "行数": body.count("\n") + 1,
            "段落数": len([p for p in body.split("\n\n") if p.strip()]),
        })
    if len(rows) < 3:
        # 兜底一：没有小标题的长文，按段落切分，让"段落字数"这类分析能出图
        text = (markdown or "").strip()
        paras = [p.strip() for p in re.split(r"\n\s*\n", text) if len(p.strip()) >= 20]
        if len(paras) >= 4:
            rows = []
            for i, p in enumerate(paras[:60], 1):
                rows.append({
                    "段落序号": i,
                    "字数": len(p),
                    "句数": max(1, p.count("。") + p.count("！") + p.count("？") + p.count(".")),
                    "含数字": int(bool(re.search(r"\d", p))),
                    "行数": p.count("\n") + 1,
                })
        elif text:
            # 兜底二：内容本身太少，只能出一行，画不了图
            rows = [{"章节": "整页正文", "字数": len(text), "行数": text.count("\n") + 1,
                     "段落数": len([p for p in text.split("\n\n") if p.strip()])}]
    return pd.DataFrame(rows) if rows else None


# ---------------------------------------------------------------- 空壳页 / 反爬识别
# 这些站点抓到的不是数据，是"请登录""加载中""安全验证"——必须说人话，
# 否则用户只会看到"未抽取到可绘图的结构化数据"，根本不知道是站点拦了它。
BLOCKED_HINTS = [
    ("加载中", "页面显示「加载中」：数据是前端异步加载的，爬虫拿到的是空壳"),
    ("请登录", "页面要求登录后才能看内容"),
    ("扫码登录", "页面要求扫码登录"),
    ("安全验证", "触发了站点的安全验证（滑块/人机校验）"),
    ("验证码", "页面要求输入验证码"),
    ("访问受限", "站点限制访问"),
    ("captcha", "站点要求人机验证（captcha）"),
    ("access denied", "站点拒绝访问"),
    ("just a moment", "Cloudflare 人机校验拦截"),
    ("sign in", "页面要求登录"),
    ("log in to continue", "页面要求登录"),
    ("robot", "站点判定为机器人访问"),
]


def detect_blocked(markdown: str, html: str = "") -> str:
    """判断抓回来的是不是空壳页/登录页，返回给人看的说明；正常页面返回空串

    判据：同时命中 ≥2 个不同信号（例如「加载中」+「请登录」），或页面正文极短。
    只用单个关键词会误判——科技文章里出现 robot、教程里出现"验证码"都很正常。
    """
    text = ((markdown or "") + "\n" + (html or "")[:30000]).lower()
    hits = [(kw, msg) for kw, msg in BLOCKED_HINTS if kw.lower() in text]
    if not hits:
        return ""
    if len(hits) >= 2 or len(markdown or "") < 1200:
        return hits[0][1]
    return ""


# ---------------------------------------------------------------- arXiv 适配器
def is_bookschina(url: str) -> bool:
    return "bookschina.com" in url.lower()


def extract_bookschina(soup) -> pd.DataFrame:
    """中国图书网（bookschina.com）商品列表适配器：书名 / 到手价 / 原价 / 折扣率

    结构：div.bDetai（每个商品一块）→ div.bName a（书名+链接），
    span.sellPrice（到手价，如 ¥11.7）、span.price（原价，如 ¥45.0）。
    首页头条大卡是 div.mainEditorInner，结构里书名在 h2 a，两种都兼容。
    """
    blocks = soup.select("div.bDetai") or soup.select("div.mainEditorInner") or soup.select("div.mainText")
    if len(blocks) < 5:
        return None

    def price(el, cls):
        node = el.find("span", class_=cls)
        if not node:
            return None
        m = re.search(r"\d+(?:\.\d+)?", node.get_text(strip=True).replace(",", ""))
        return float(m.group(0)) if m else None

    rows = []
    for el in blocks:
        a = el.select_one("div.bName a") or el.select_one("h2 a")
        if a is None:
            a = el.find("a", href=True)
        title = (a.get_text(" ", strip=True) if a else "")
        if not title:
            continue
        sell = price(el, "sellPrice")
        origin = price(el, "price")
        href = (a.get("href") if a else "") or ""
        if href.startswith("//"):
            href = "https:" + href
        desc = ""
        node = el.find("div", class_="mainFont")
        if node:
            desc = node.get_text(" ", strip=True)
        rows.append({
            "书名": title[:120],
            "到手价": sell,
            "原价": origin,
            "折扣率": (round(sell / origin, 3) if sell and origin else None),
            "简介字数": len(desc) if desc else None,
            "链接": href[:200],
        })
    if len(rows) < 5:
        return None
    df = pd.DataFrame(rows)
    if df["到手价"].notna().sum() < 5:
        return None
    return df


def is_arxiv_list(url: str) -> bool:
    return "arxiv.org/list/" in url


def extract_arxiv(html: str) -> pd.DataFrame:
    from src.crawl_svc import parse_arxiv_listing, clean_papers

    records = parse_arxiv_listing(html)
    if not records:
        return None
    clean_df, _ = clean_papers(pd.DataFrame(records), category="arxiv")
    keep = ["paper_id", "title", "author_count", "first_author", "primary_subject", "month", "title_len", "has_code"]
    df = clean_df[[c for c in keep if c in clean_df.columns]]
    df = df.rename(columns={
        "paper_id": "论文编号", "title": "标题", "author_count": "作者数",
        "first_author": "第一作者", "primary_subject": "主分类", "month": "月份",
        "title_len": "标题字数", "has_code": "有开源代码",
    })
    return df if len(df) >= 3 else None


def is_dangdang_list(url: str) -> bool:
    """当当分类页（/cp01.xx...html）——国内少数不需要登录态就能拿到商品价格与评分的列表页"""
    u = url.lower()
    return "dangdang.com" in u and ("/cp" in u or "/category" in u or "list" in u)


def extract_dangdang(soup) -> pd.DataFrame:
    """当当商品列表适配器：标题 / 现价 / 定价 / 折扣 / 评论数

    为什么单独写：商品块里价格是 <span class="search_now_price">¥56.60</span>，
    通用抽取只认「¥56.60」这种连续文本，遇到被 span 拆开或中间插空格的写法就抽不到，
    于是价格列全空、画不出价格分布图。
    """
    blocks = [li for li in soup.find_all("li")
              if li.get("class") and any(c.startswith("line") for c in li.get("class"))]
    if len(blocks) < 5:
        blocks = [li for li in soup.find_all("li") if li.find("p", class_="name")]
    if len(blocks) < 5:
        return None

    def money(el, cls):
        node = el.find("span", class_=cls)
        if not node:
            return None
        m = re.search(r"\d+(?:\.\d+)?", node.get_text(strip=True).replace(",", ""))
        return float(m.group(0)) if m else None

    rows = []
    for el in blocks:
        name = el.find("p", class_="name")
        title = name.get_text(" ", strip=True) if name else ""
        if not title:
            continue
        now = money(el, "search_now_price")
        pre = money(el, "search_pre_price")
        discount = None
        dnode = el.find("span", class_="search_discount")
        if dnode:
            m = re.search(r"(\d+(?:\.\d+)?)\s*折", dnode.get_text(strip=True))
            discount = float(m.group(1)) if m else None
        comments = None
        cnode = el.find("a", class_="search_comment_num")
        if cnode:
            m = re.search(r"(\d+)", cnode.get_text(strip=True).replace(",", ""))
            comments = int(m.group(1)) if m else None
        link = el.find("a", href=True)
        rows.append({
            "商品标题": title[:120],
            "现价": now,
            "定价": pre,
            "折扣": discount,
            "评论数": comments,
            "链接": ("https:" + link["href"]) if link and link["href"].startswith("//") else (link["href"] if link else ""),
        })
    if len(rows) < 5:
        return None
    df = pd.DataFrame(rows)
    # 价格抽不到就别装成能分析，直接判失败回落到通用抽取
    if df["现价"].notna().sum() < 5:
        return None
    return df


# ---------------------------------------------------------------- DataFrame 打磨
def polish(df: pd.DataFrame, max_rows: int = 300, max_cols: int = 12) -> pd.DataFrame:
    """把网页里抽出来的脏表整理成 LIDA 能吃的表：
    只留可序列化的列、去掉全空列、把形如价格的文本列转成数值、限制行列规模
    """
    if df is None or df.empty:
        return df
    df = df.copy()
    df = df.loc[:, [c for c in df.columns if str(c).strip()]]
    df = df.dropna(axis=1, how="all")

    for c in list(df.columns):
        s = df[c]
        if s.dtype == object:
            # 全是 list/dict 的列直接扔掉，无法分析
            if s.map(lambda v: isinstance(v, (list, dict, set))).any():
                df = df.drop(columns=[c])
                continue
            # 尝试转数值（价格、评分这类）
            num = pd.to_numeric(
                s.astype(str).str.replace(r"[^\d.\-]", "", regex=True).replace({"": None}), errors="coerce"
            )
            if num.notna().mean() >= 0.7:
                df[c] = num

    if len(df.columns) > max_cols:
        df = df.iloc[:, :max_cols]
    if len(df) > max_rows:
        df = df.head(max_rows)
    return df.reset_index(drop=True)


def analysis_view(df: pd.DataFrame) -> pd.DataFrame:
    """给分析用的视图：剔掉"每行都不同、又没法统计"的列（链接、编号、长文本 ID），
    这类列喂给大模型只会引出"各链接数量分布"这种没意义的图
    """
    if df is None or df.empty:
        return df
    drop = []
    for c in df.columns:
        s = df[c].astype(str)
        uniq = s.nunique(dropna=True) / max(len(s), 1)
        avg_len = s.str.len().mean()
        name = str(c)
        if name in ("链接", "url", "URL", "条目链接"):
            drop.append(c)
        elif uniq >= 0.85 and avg_len >= 25:
            drop.append(c)
    keep = [c for c in df.columns if c not in drop]
    if len(keep) < 2:          # 别把表删空了
        keep = list(df.columns)
    return df[keep]


# 只允许"类别字段的数量占比"，不允许对连续数值求占比/趋势（这类图在语义上就是错的）
_BAD_WORDS = ("占比", "比例", "百分比", "增长率", "趋势", "环比", "同比")


def sanitize_goals(adf: pd.DataFrame, goals: list) -> list:
    """把大模型提的、数据上站不住脚的目标过滤掉（确定性规则，不靠模型自觉）"""
    if adf is None or adf.empty:
        return goals
    numeric_cols = [str(c) for c in adf.columns if pd.api.types.is_numeric_dtype(adf[c])]
    keep = []
    for g in goals:
        g = str(g).strip()
        if not g:
            continue
        hit_numeric = [c for c in numeric_cols if c in g]
        if "趋势" in g or "环比" in g or "同比" in g:
            continue
        if hit_numeric and any(w in g for w in _BAD_WORDS):
            continue
        if g not in keep:
            keep.append(g)
    return keep


def default_goal(df: pd.DataFrame):
    """没有大模型时也能给出一个像样的分析目标"""
    if df is None or df.empty:
        return None
    for c in df.columns:
        if df[c].dtype == object:
            nun = df[c].nunique(dropna=True)
            if 2 <= nun <= 30:
                return f"统计各{c}的数量分布并画柱状图"
    for c in df.columns:
        if pd.api.types.is_numeric_dtype(df[c]):
            return f"统计{c}的分布并画直方图"
    return "统计每行文本长度分布并画直方图"


def schema_desc(df: pd.DataFrame, rows: int = 3) -> tuple:
    lines = [f"- {c}（{df[c].dtype}，{df[c].nunique(dropna=True)} 个不同值）" for c in df.columns]
    sample = df.head(rows).to_string(index=False, max_colwidth=24)
    return "\n".join(lines), sample


# ---------------------------------------------------------------- 主流程
async def run_page(url: str, goal: str = None, want_summary: bool = True) -> dict:
    """抓取 → 抽取 → 落盘 → 摘要 → 自动出分析目标 → 出图，返回完整结果"""
    os.makedirs(MD_DIR, exist_ok=True)
    os.makedirs(EXT_DIR, exist_ok=True)

    if not url.startswith(("http://", "https://")):
        url = "https://" + url

    crawled = await crawl_any(url)
    if not crawled["ok"]:
        page_id = uuid.uuid4().hex[:12]
        save_page(page_id, url, "", "failed", "", "", 0, 0, 0, [], status="failed")
        update_page(page_id, status="failed", error=crawled["error"])
        return {"ok": False, "page_id": page_id, "url": url, "error": crawled["error"]}

    soup = BeautifulSoup(crawled["html"], "html.parser")
    title = crawled["title"] or (soup.title.get_text(strip=True) if soup.title else "") or url

    # 三级抽取
    adapter = "generic"
    df = None
    table_count = 0

    payload = crawled.get("json")
    if payload is not None:
        df = json_to_df(payload)
        adapter = "json_api" if df is not None else "generic"

    if df is None and is_arxiv_list(url):
        df = extract_arxiv(crawled["html"])
        adapter = "arxiv" if df is not None else "generic"

    if df is None and is_dangdang_list(url):
        df = extract_dangdang(soup)
        adapter = "dangdang" if df is not None else "generic"

    if df is None and is_bookschina(url):
        df = extract_bookschina(soup)
        adapter = "bookschina" if df is not None else "generic"

    if df is None:
        tables = extract_tables(soup)
        table_count = len(tables)
        if tables:
            df = tables[0]
            adapter = "table"

    if df is None:
        items = extract_repeated_items(soup, url)
        if items is not None:
            df = items
            adapter = "items"

    if df is None:
        df = extract_text_blocks(crawled["markdown"])
        adapter = "text_blocks"

    df = polish(df) if df is not None else None

    page_id = uuid.uuid4().hex[:12]
    md_path = os.path.join(MD_DIR, f"{page_id}.md")
    with open(md_path, "w", encoding="utf-8") as f:
        f.write(crawled["markdown"] or "")

    parquet_path = ""
    if df is not None and not df.empty:
        parquet_path = os.path.join(EXT_DIR, f"{page_id}.parquet")
        try:
            df.to_parquet(parquet_path, index=False)
        except Exception:
            # parquet 引擎不可用时退回 CSV，保证前端预览一定有数据
            parquet_path = os.path.join(EXT_DIR, f"{page_id}.csv")
            try:
                df.to_csv(parquet_path, index=False, encoding="utf-8-sig")
            except Exception:
                parquet_path = ""

    save_page(
        page_id=page_id, url=url, title=title[:300], adapter=adapter,
        markdown_path=md_path, parquet_path=parquet_path,
        table_count=table_count,
        row_count=int(len(df)) if df is not None else 0,
        col_count=int(len(df.columns)) if df is not None else 0,
        columns=[str(c) for c in (df.columns if df is not None else [])],
    )

    # 摘要（语义活，交给大模型；失败降级成截断正文）
    summary = ""
    if want_summary:
        try:
            summary = summarize_page(title, crawled["markdown"][:20000])
        except Exception as e:
            head = re.sub(r"\s+", " ", crawled["markdown"] or "")[:400]
            summary = f"（大模型摘要不可用：{type(e).__name__}）\n\n正文开头：{head}"

    # 分析目标：用户给了就用，没给就自动提
    adf = analysis_view(df) if df is not None else None
    goals = []
    if goal:
        goals = [goal]
    else:
        try:
            s_desc, sample = schema_desc(adf) if adf is not None and not adf.empty else ("（无结构化数据）", "")
            goals = propose_goals(title, s_desc, sample, n=3)
        except Exception:
            goals = []
        if not goals:
            g = default_goal(adf)
            goals = [g] if g else []

    # 过滤掉数据上站不住的目标；至少保证 1 个
    if not goal and goals:
        goals = sanitize_goals(adf, goals)
        if not goals:
            g = default_goal(adf)
            goals = [g] if g else []

    update_page(page_id, summary=summary, goals=goals, status="analyzing")

    # 出图：默认最多 3 个目标各出一张（PAGE_MAX_CHARTS 可调）
    charts = []
    chart_error = None
    rows_n = int(len(df)) if df is not None else 0
    blocked = detect_blocked(crawled["markdown"], crawled.get("html", "")[:50000])
    max_charts = int(os.getenv("PAGE_MAX_CHARTS") or 3)
    if blocked and adapter == "text_blocks":
        # 抓回来是空壳页：直接说清楚是站点拦了，不是你的用法有问题，也别浪费 token 去画图
        chart_error = (f"抓到的不是数据页：{blocked}。"
                       f"淘宝/京东/拼多多/小红书这类站点需要登录态且有人机校验，公开爬取拿不到商品列表；"
                       f"换成公开列表页（books.toscrape.com、sspai.com、arXiv）才能出图")
    elif adf is not None and len(adf) >= 3 and goals:
        try:
            from src.lida_svc import analyze_multi_goals

            _, charts, errs = analyze_multi_goals(
                adf, goals[:max_charts], library="seaborn", prefix=f"page_{page_id}"
            )
            chart_error = "；".join(errs) if errs else None
        except Exception as e:
            chart_error = f"{type(e).__name__}: {e}"
    elif blocked and rows_n < 3:
        # 抓回来是空壳页：直接说清楚是站点拦了，不是你的用法有问题
        chart_error = (f"抓到的不是数据页：{blocked}。"
                       f"淘宝/京东/拼多多/小红书这类站点需要登录态且有人机校验，公开爬取拿不到商品列表；"
                       f"换成公开列表页（books.toscrape.com、sspai.com、arXiv）才能出图")
    elif rows_n < 3:
        hint = "这是纯正文页，按段落切分后仍不足 3 行" if adapter == "text_blocks" else "页面里没有重复块或表格"
        chart_error = (f"只抽到 {rows_n} 行数据，画图至少需要 3 行（{hint}）。"
                       f"想看图表请换列表型/表格型页面；正文页可以先看上面的内容摘要")
    else:
        chart_error = "没有可执行的分析目标（数据里没有数值/类别类字段可比）"

    update_page(page_id, charts=charts, status="done", error=chart_error if not charts else None)

    return {
        "ok": True,
        "page_id": page_id,
        "url": url,
        "title": title[:300],
        "adapter": adapter,
        "table_count": table_count,
        "rows": int(len(df)) if df is not None else 0,
        "cols": [str(c) for c in (df.columns if df is not None else [])],
        "summary": summary,
        "goals": goals,
        "charts": [{**c, "image_path": public_path(c.get("image_path", ""))} for c in charts],
        "chart_error": chart_error,
        "blocked_reason": blocked or "",
        "markdown_path": public_path(md_path),
        "parquet_path": public_path(parquet_path),
        "fetched_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


def load_page_records(page_id: str, limit: int = 20) -> list:
    """从落盘的 parquet / csv 读回抽取结果，给前端表格预览"""
    from src.db import get_page

    p = get_page(page_id)
    if not p:
        return []
    path = p.get("parquet_path") or ""
    if not path or not os.path.isfile(path):
        base = os.path.join(EXT_DIR, page_id)
        for ext in (".parquet", ".csv"):
            if os.path.isfile(base + ext):
                path = base + ext
                break
    if not path or not os.path.isfile(path):
        return []
    try:
        if path.endswith(".csv"):
            df = pd.read_csv(path).head(limit)
        else:
            df = pd.read_parquet(path).head(limit)
        return json.loads(df.to_json(orient="records", force_ascii=False, default_handler=str))
    except Exception:
        return []
