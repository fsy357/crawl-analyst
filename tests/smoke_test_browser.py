# 浏览器可用性冒烟测试：确认 Playwright + Chromium 能正常启动
from playwright.sync_api import sync_playwright

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    print("浏览器版本:", browser.version)
    page = browser.new_page()
    page.goto("data:text/html,<h1>ok-browser-works</h1>")
    print("页面读取:", page.inner_text("h1"))
    browser.close()
    print("浏览器启动测试通过")
