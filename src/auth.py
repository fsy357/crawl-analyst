"""公网访问基础鉴权（HTTP Basic Auth）

为什么必须有：这个服务"提交一次就是一次真金白银的大模型调用"，
公网裸奔等于把自己 API key 的额度开放给所有人。限流只能降低单位时间消耗，
挡不住"每天 200 次"这种合规范围内的额度消耗——所以对外暴露必须加一道闸。

用法（.env）：
    PUBLIC_AUTH=1                 # 打开鉴权；本地开发不设即可免登录
    PUBLIC_USER=demo
    PUBLIC_PASS=换成你自己的密码

浏览器会弹系统登录框，输入用户名密码即可；也可以用
    curl -u demo:密码 http://host:8000/api/pages
"""
import base64
import os
import secrets

from fastapi import Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

# 放行的路径：健康检查要能给探针/监控用，否则容器 HEALTHCHECK 会失败
EXEMPT_PATHS = ("/api/health",)


def enabled() -> bool:
    return (os.getenv("PUBLIC_AUTH") or "0").strip() in ("1", "true", "True", "yes")


def _credentials() -> tuple:
    return ((os.getenv("PUBLIC_USER") or "demo").strip(),
            (os.getenv("PUBLIC_PASS") or "").strip())


class BasicAuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        if not enabled():
            return await call_next(request)
        if request.url.path in EXEMPT_PATHS:
            return await call_next(request)

        user, pwd = _credentials()
        if not pwd:
            # 开着鉴权却没配密码，直接拒绝启动期的请求，避免"以为保护了其实是空密码"
            return JSONResponse({"detail": "服务未配置 PUBLIC_PASS，已拒绝访问"}, status_code=503)

        auth = request.headers.get("authorization") or ""
        if auth.lower().startswith("basic "):
            try:
                raw = base64.b64decode(auth.split(" ", 1)[1]).decode("utf-8")
                u, _, p = raw.partition(":")
                if secrets.compare_digest(u, user) and secrets.compare_digest(p, pwd):
                    return await call_next(request)
            except Exception:
                pass

        return JSONResponse(
            {"detail": "需要登录：请输入访问用户名和密码"},
            status_code=401,
            headers={"WWW-Authenticate": 'Basic realm="crawl-analyst"'},
        )
