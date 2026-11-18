# LIDA 冒烟测试：验证 DeepSeek 能驱动 LIDA 生成图表
import sys
sys.path.insert(0, "D:/crawl_analyst")

import pandas as pd
from src.lida_svc import analyze_dataframe

# 用模拟的爬虫结果搭一张表（后面会换成真实爬取的数据）
df = pd.DataFrame(
    {
        "title": ["Book A", "Book B", "Book C", "Book D", "Book E", "Book F"],
        "price": [10.5, 22.0, 15.8, 35.2, 9.9, 48.0],
        "rating": ["Three", "Four", "Three", "Five", "Two", "Five"],
        "category": ["Travel", "Poetry", "Fiction", "Travel", "Fiction", "Poetry"],
    }
)

print("输入数据行数:", len(df))
summary, charts = analyze_dataframe(
    df,
    goal="统计不同 rating 等级的书本数量，并画一张柱状图",
    library="seaborn",
)

print("\n字段画像字段数:", len(summary.get("fields") or []))
for i, c in enumerate(charts):
    print(f"\n=== 图表 {i} ===")
    print("生成成功:", c["status"])
    if c["error"]:
        print("报错:", str(c["error"])[:300])
    print("图片路径:", c["image_path"])
    print("生成的代码:")
    print(c["code"][:600])
