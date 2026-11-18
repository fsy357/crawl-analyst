"""限流：三层，目的是"保护钱包和机器"，不是限制正常用户

1) **单 IP**：每分钟最多 N 次提交 —— 防一个人写脚本刷
2) **单域名**：每分钟最多 N 次提交同一个站点 —— 防你把别人站打挂、也防单站重复烧 token
3) **全局每日预算**：一天最多提交 N 次 —— 防零散来源合起来把 API key 刷爆

为什么用内存计数而不是 Redis：限流本来就是"本机进程级别的粗粒度保护"，
进程重启后归零可以接受；单机部署时内存实现零依赖、零额外延迟。
如果以后多实例部署，把 _store 换成 Redis 的 INCR + EXPIRE 即可，接口不用改。
"""
import os
import threading
import time
from collections import defaultdict, deque


class RateLimited(Exception):
    """被限流，reason 直接展示给用户，retry_after 秒后重试"""

    def __init__(self, reason: str, retry_after: int = 60):
        super().__init__(reason)
        self.reason = reason
        self.retry_after = int(retry_after)


_lock = threading.Lock()
_store = {
    "ip": defaultdict(deque),     # ip -> [时间戳...]
    "host": defaultdict(deque),   # host -> [时间戳...]
    "day": "",                    # 当前统计日期
    "day_count": 0,               # 当日提交总数
}


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name) or default)
    except ValueError:
        return default


def ip_per_min() -> int:
    return _env_int("RL_IP_PER_MIN", 3)


def host_per_min() -> int:
    return _env_int("RL_HOST_PER_MIN", 5)


def daily_budget() -> int:
    return _env_int("RL_DAILY_TASKS", 200)


def _prune(dq: deque, window_sec: int, now: float):
    while dq and now - dq[0] > window_sec:
        dq.popleft()


def check(client_ip: str, host: str):
    """提交前调用；超限抛 RateLimited"""
    now = time.time()
    today = time.strftime("%Y-%m-%d")

    with _lock:
        if _store["day"] != today:
            _store["day"] = today
            _store["day_count"] = 0

        if _store["day_count"] >= daily_budget():
            raise RateLimited(
                f"今日全站分析额度已用完（{daily_budget()} 次/天），明天再来或调高 RL_DAILY_TASKS",
                retry_after=3600,
            )

        ip_dq = _store["ip"][client_ip]
        _prune(ip_dq, 60, now)
        if len(ip_dq) >= ip_per_min():
            wait = int(60 - (now - ip_dq[0])) + 1
            raise RateLimited(
                f"提交太频繁：同一来源每分钟最多 {ip_per_min()} 次，请 {wait} 秒后重试",
                retry_after=wait,
            )

        host_dq = _store["host"][host]
        _prune(host_dq, 60, now)
        if len(host_dq) >= host_per_min():
            wait = int(60 - (now - host_dq[0])) + 1
            raise RateLimited(
                f"同一站点提交太频繁：每分钟最多 {host_per_min()} 次，请 {wait} 秒后重试",
                retry_after=wait,
            )

        # 通过才计数（被安全校验拒绝的请求不该占额度）
        ip_dq.append(now)
        host_dq.append(now)
        _store["day_count"] += 1


def stats() -> dict:
    with _lock:
        return {
            "day": _store["day"],
            "day_count": _store["day_count"],
            "day_budget": daily_budget(),
            "ip_per_min": ip_per_min(),
            "host_per_min": host_per_min(),
            "ips_tracked": len(_store["ip"]),
            "hosts_tracked": len(_store["host"]),
        }
