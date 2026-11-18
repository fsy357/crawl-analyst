"""LIDA 数据分析智能体封装（适配 DeepSeek 等 OpenAI 兼容接口）

对外只暴露两个函数，方便以后换成别的分析智能体：
- build_manager()      -> 返回 LIDA 的 Manager
- analyze_dataframe()  -> 输入 DataFrame + 中文分析需求，返回图表与分析结果

为什么要在 lida_svc 里包一层：lida 0.0.14 的 Manager 只接受 llmx 的 TextGenerator，
而 llmx 的 OpenAI 生成器没有暴露 base_url 参数，默认只能连官方 OpenAI。
这里继承它并替换 client，就能指向 DeepSeek。
"""
import os
import pandas as pd
from dotenv import load_dotenv
from diskcache import Cache
from openai import OpenAI
from lida import Manager, TextGenerationConfig
from llmx.generators.text.openai_textgen import OpenAITextGenerator

# 图表里的中文需要中文字体，否则会显示成方块。
# Windows 有雅黑/黑体，Linux 容器里要装 fonts-noto-cjk（见 deploy/Dockerfile）。
import matplotlib
matplotlib.use("Agg")

_CJK_FONTS = [
    "Microsoft YaHei", "SimHei", "DengXian",          # Windows
    "Noto Sans CJK SC", "Noto Sans CJK JP",           # Linux（Debian: fonts-noto-cjk）
    "Source Han Sans SC", "WenQuanYi Zen Hei",        # 其他 Linux 发行版
    "DejaVu Sans",                                     # 兜底（无中文）
]


def pick_cjk_font() -> str:
    """挑一个当前系统真实存在的字体，避免设了不存在的字体导致中文全变方块"""
    try:
        from matplotlib.font_manager import fontManager

        available = {f.name for f in fontManager.ttflist}
        for name in _CJK_FONTS:
            if name in available:
                return name
    except Exception:
        pass
    return "DejaVu Sans"


CJK_FONT = pick_cjk_font()
matplotlib.rcParams["font.sans-serif"] = [CJK_FONT, "DejaVu Sans"]
matplotlib.rcParams["axes.unicode_minus"] = False
try:
    import seaborn as sns
    sns.set_theme(style="whitegrid")
    sns.set(font=CJK_FONT)
except Exception:
    pass

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(BASE_DIR, ".env"))

CACHE_DIR = os.path.join(BASE_DIR, "data", "lida_cache")
CHART_DIR = os.path.join(BASE_DIR, "data", "charts")


class OpenAICompatTextGenerator(OpenAITextGenerator):
    """llmx 的 OpenAI 生成器不接收 base_url，继承后替换 client 指向自定义服务"""

    def __init__(self, api_key: str, base_url: str, model: str):
        super().__init__(api_key=api_key, provider="openai", model=model)
        self.client = OpenAI(api_key=api_key, base_url=base_url)


def build_text_gen() -> OpenAICompatTextGenerator:
    api_key = (os.getenv("LLM_API_KEY") or "").strip()
    if not api_key:
        raise RuntimeError("LLM_API_KEY 未配置，请在 D:\\crawl_analyst\\.env 里填上")
    gen = OpenAICompatTextGenerator(
        api_key=api_key,
        base_url=os.getenv("LLM_BASE_URL") or "https://api.deepseek.com/v1",
        model=os.getenv("LLM_MODEL") or "deepseek-chat",
    )
    # llmx 默认把缓存写到 C 盘用户目录，这里改到 D 盘
    os.makedirs(CACHE_DIR, exist_ok=True)
    gen.cache_dir = CACHE_DIR
    gen.cache = Cache(CACHE_DIR)
    return gen


def build_manager() -> Manager:
    return Manager(text_gen=build_text_gen())


def analyze_dataframe(
    df: pd.DataFrame,
    goal: str,
    library: str = "seaborn",
    n_charts: int = 1,
    save_dir: str = CHART_DIR,
    prefix: str = "chart",
):
    """对 DataFrame 做分析并生成图表。

    参数:
        df       : 结构化数据（爬虫清洗后的表）
        goal     : 中文分析需求，例如「统计各评分等级的书本数量并画柱状图」
        library  : seaborn / matplotlib / altair
        n_charts : 让模型生成几张图表方案

    返回:
        (summary_dict, charts)
        summary_dict : 数据集画像（字段名、类型、样例值）
        charts       : 列表，每项含 status / code / spec / error / image_path
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError("df 必须是 pandas.DataFrame")
    if df.empty:
        raise ValueError("df 为空，没有可分析的数据")

    manager = build_manager()
    config = TextGenerationConfig(
        n=1,
        temperature=0,
        model=os.getenv("LLM_MODEL") or "deepseek-chat",
    )

    # summarize 同时会把 df 挂到 manager.data 上，后面 visualize 要用
    summary = manager.summarize(data=df, textgen_config=config)
    charts_raw = manager.visualize(
        summary=summary,
        goal=goal,
        textgen_config=config,
        library=library,
    )

    os.makedirs(save_dir, exist_ok=True)
    charts = []
    for i, chart in enumerate(charts_raw[:n_charts]):
        image_path = None
        if getattr(chart, "raster", None):
            image_path = os.path.join(save_dir, f"{prefix}_{i}.png")
            chart.savefig(image_path)
        charts.append(
            {
                "status": chart.status,
                "code": chart.code,
                "spec": chart.spec,
                "error": chart.error,
                "image_path": image_path,
            }
        )

    summary_dict = {
        "name": getattr(summary, "name", "dataframe"),
        "fields": getattr(summary, "fields", None),
    }
    return summary_dict, charts


def analyze_multi_goals(
    df: pd.DataFrame,
    goals: list,
    library: str = "seaborn",
    save_dir: str = CHART_DIR,
    prefix: str = "chart",
    per_goal: int = 1,
):
    """多个分析目标各出一张图，但**数据集画像只算一次**。

    为什么要这样：manager.summarize() 本身是一次大模型调用，如果每个目标都调
    analyze_dataframe()，N 个目标就是 N 次 summarize + N 次 visualize。
    这里 summarize 一次、visualize N 次，token 和耗时都省一半以上。

    返回 (summary_dict, charts, errors)
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError("df 必须是 pandas.DataFrame")
    if df.empty:
        raise ValueError("df 为空，没有可分析的数据")

    manager = build_manager()
    config = TextGenerationConfig(
        n=1,
        temperature=0,
        model=os.getenv("LLM_MODEL") or "deepseek-chat",
    )
    summary = manager.summarize(data=df, textgen_config=config)

    os.makedirs(save_dir, exist_ok=True)
    charts, errors = [], []
    for gi, goal in enumerate(goals):
        try:
            charts_raw = manager.visualize(
                summary=summary, goal=goal, textgen_config=config, library=library
            )
        except Exception as e:
            errors.append(f"{goal} → {type(e).__name__}: {e}")
            continue
        for j, chart in enumerate(charts_raw[:per_goal]):
            image_path = None
            if getattr(chart, "raster", None):
                image_path = os.path.join(save_dir, f"{prefix}_g{gi}_{j}.png")
                try:
                    chart.savefig(image_path)
                except Exception as e:
                    errors.append(f"{goal} 存图失败 → {type(e).__name__}: {e}")
                    image_path = None
            charts.append({
                "goal": goal,
                "status": chart.status,
                "code": chart.code,
                "spec": chart.spec,
                "error": chart.error,
                "image_path": image_path,
            })
            if not image_path:
                errors.append(f"{goal} → 未渲染出图片：{getattr(chart, 'error', '')}")

    summary_dict = {
        "name": getattr(summary, "name", "dataframe"),
        "fields": getattr(summary, "fields", None),
    }
    return summary_dict, charts, errors
