"""大模型调用层（DeepSeek / OpenAI 兼容接口）

只在“需要理解语义”的地方用：
- 页面正文摘要
- 根据数据结构自动提议分析目标

确定性的事（抓取、解析、清洗、统计）一律不用大模型，避免幻觉污染数据。
"""
import os
from dotenv import load_dotenv
from openai import OpenAI

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(BASE_DIR, ".env"))

_client = None


def get_client() -> OpenAI:
    global _client
    if _client is None:
        api_key = (os.getenv("LLM_API_KEY") or "").strip()
        if not api_key:
            raise RuntimeError("LLM_API_KEY 未配置")
        _client = OpenAI(
            api_key=api_key,
            base_url=os.getenv("LLM_BASE_URL") or "https://api.deepseek.com/v1",
            timeout=60,
        )
    return _client


def model_name() -> str:
    return os.getenv("LLM_MODEL") or "deepseek-chat"


def chat(messages, temperature: float = 0.0, max_tokens: int = 800) -> str:
    """一次普通对话调用，返回纯文本"""
    resp = get_client().chat.completions.create(
        model=model_name(),
        messages=messages,
        temperature=temperature,
        max_tokens=max_tokens,
    )
    return (resp.choices[0].message.content or "").strip()


def summarize_page(title: str, text: str, max_chars: int = 12000) -> str:
    """给网页正文写中文摘要；超长文本先分段摘要再合并"""
    text = (text or "").strip()
    if not text:
        return "（页面没有可读取的正文）"

    if len(text) <= max_chars:
        return chat([
            {"role": "system", "content": "你是网页内容分析助手。只依据给定文本作答，不推测文本之外的信息。"},
            {"role": "user", "content": (
                f"网页标题：{title}\n\n网页正文：\n{text}\n\n"
                "请用中文输出：\n"
                "1）一句话说明这个页面是什么；\n"
                "2）3~5 条要点（每条不超过 40 字）；\n"
                "3）能用数据描述的事实（如数量、价格区间、时间范围），没有就写“无”。\n"
                "不要编造数字。"
            )},
        ], max_tokens=900)

    # 超长：切成若干段分别摘要，再做二次合并
    chunk_size = 6000
    parts = [text[i:i + chunk_size] for i in range(0, min(len(text), 60000), chunk_size)]
    partials = []
    for i, part in enumerate(parts[:8]):
        partials.append(chat([
            {"role": "system", "content": "你是网页内容分析助手，只概括给定片段，不猜测。"},
            {"role": "user", "content": f"这是网页的第 {i+1} 段内容，请用中文写 2~4 条要点：\n{part}"},
        ], max_tokens=300))
    merged = "\n".join(f"[片段{i+1}]\n{p}" for i, p in enumerate(partials))
    return chat([
        {"role": "system", "content": "你是网页内容分析助手，只整合给定片段，不添加片段之外的信息。"},
        {"role": "user", "content": (
            f"网页标题：{title}\n\n各片段要点：\n{merged}\n\n"
            "请合并成：1）一句话说明页面是什么；2）3~5 条整体要点；3）文本中出现的可用数据事实。不要编造。"
        )},
    ], max_tokens=900)


def propose_goals(title: str, schema_desc: str, sample: str, n: int = 3) -> list:
    """根据数据结构自动提议分析目标，返回中文目标列表"""
    raw = chat([
        {"role": "system", "content": "你是数据分析师，负责根据表格结构提出可用图表回答的问题。只提数据结构支持的问题。"},
        {"role": "user", "content": (
            f"网页标题：{title}\n\n数据结构：\n{schema_desc}\n\n样例数据：\n{sample}\n\n"
            f"请提出 {n} 个可以用图表回答的中文分析目标，每个不超过 20 字。\n"
            "要求：\n"
            "1）优先用“各类别的数量分布 / 占比 / 分组统计”，这类目标一定能画出有效图表；\n"
            "2）只允许用上面真实存在的字段，不要编造字段，不要出现英文列名；\n"
            "3）三个目标要落在不同维度上，不要重复，也不要提“两个本来就很接近的字段做对比”这种图；\n"
            "4）不要提趋势类目标（这类数据没有时间列）。\n"
            "每行一个，用数字编号开头，不要解释。"
        )},
    ], max_tokens=300)

    goals = []
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        line = line.lstrip("0123456789.、) ）-*").strip()
        if line and len(line) <= 40:
            goals.append(line)
    return goals[:n]
