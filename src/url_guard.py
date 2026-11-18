"""URL 准入校验：开放任意公网站点，只拦"危险地址"

设计取舍（面试讲点）：
- **不做域名白名单**：产品定位是"输入任意公开网址就能分析"，白名单会直接把功能阉割掉。
  白名单只作为 demo / 私有部署的**可选开关**（PAGE_URL_POLICY），不是默认策略。
- **做危险地址黑名单**：协议、端口、内网 IP、重定向二次校验。
  这些拦的全是"攻击面"，不会误伤任何正常网站——正常站点都在 80/443、都是公网 IP。

四道校验（顺序有讲究，先便宜的再贵的）：
1) 协议白名单 → 2) 端口白名单 → 3) 策略开关（demo/allowlist 时才查域名）
→ 4) DNS 解析后逐个 IP 判私网（防 SSRF，这一步最贵，放最后）
"""
import ipaddress
import os
import socket
from urllib.parse import urlparse

ALLOWED_SCHEMES = {"http", "https"}

# 只放 80/443（外加可配置的几个常用 Web 端口）。
# 关键点：拦掉 6379(Redis)/3306(MySQL)/22(SSH)/9200(ES) 这类"被当探针"的端口。
DEFAULT_PORTS = {80, 443, 8080, 8000, 8443}

# 私网 / 环回 / 链路本地 / 组播 / 保留段
# 169.254.0.0/16 里包含云平台的元数据服务 169.254.169.254，是最典型的 SSRF 目标
BLOCKED_NETS = [ipaddress.ip_network(n) for n in (
    "0.0.0.0/8", "10.0.0.0/8", "100.64.0.0/10", "127.0.0.0/8",
    "169.254.0.0/16", "172.16.0.0/12", "192.0.0.0/24", "192.168.0.0/16",
    "198.18.0.0/15", "224.0.0.0/4", "240.0.0.0/4",
    "::1/128", "fc00::/7", "fe80::/10",
)]


class UrlRejected(Exception):
    """URL 被白名单/黑名单拦下。reason 直接给前端展示，要写得像人话"""

    def __init__(self, reason: str, code: str = "url_rejected"):
        super().__init__(reason)
        self.reason = reason
        self.code = code


def _env_set(name: str, default: str = "") -> set:
    raw = os.getenv(name) or default
    return {x.strip().lower() for x in raw.split(",") if x.strip()}


def policy() -> str:
    return (os.getenv("PAGE_URL_POLICY") or "open").strip().lower()


def allowed_ports() -> set:
    extra = _env_set("PAGE_ALLOWED_PORTS_EXTRA")
    return DEFAULT_PORTS | {int(p) for p in extra if p.isdigit()}


def demo_hosts() -> set:
    return _env_set("PAGE_URL_DEMO_HOSTS",
                    "books.toscrape.com,sspai.com,arxiv.org,quotes.toscrape.com")


def allowlist_hosts() -> set:
    return _env_set("PAGE_URL_ALLOWLIST")


def _host_matches(host: str, patterns: set) -> bool:
    """支持 example.com 与其子域 a.example.com"""
    host = host.lower().rstrip(".")
    return any(host == p or host.endswith("." + p) for p in patterns)


def normalize(url: str) -> str:
    url = (url or "").strip()
    if not url:
        raise UrlRejected("请填写网址", "empty_url")
    if not url.startswith(("http://", "https://", "HTTP://", "HTTPS://")):
        # 用户常见输入：arxiv.org/list/cs.AI/recent、www.xxx.com，补协议
        if "://" in url:
            raise UrlRejected("仅支持 http/https 协议的网址", "bad_scheme")
        url = "https://" + url
    return url


def _ip_is_blocked(ip_str: str) -> bool:
    try:
        ip = ipaddress.ip_address(ip_str)
    except ValueError:
        return False
    if getattr(ip, "ipv4_mapped", None):
        ip = ip.ipv4_mapped
    return any(ip in net for net in BLOCKED_NETS)


def resolve_and_check_host(host: str):
    """把域名解析成 IP 后逐个判断，任何一个是内网地址就整体拒绝。

    为什么要解析而不是只看字符串：攻击者不会乖乖写 127.0.0.1，
    他会注册 localtest.me、或者用自己的域名解析到 127.0.0.1。
    """
    try:
        infos = socket.getaddrinfo(host, None)
    except socket.gaierror:
        raise UrlRejected(f"域名解析失败：{host}，请检查网址是否写对", "dns_fail")

    ips = {info[4][0] for info in infos if info[4] and info[4][0]}
    if not ips:
        raise UrlRejected(f"域名解析不到 IP：{host}", "dns_empty")
    for ip in ips:
        if _ip_is_blocked(ip):
            raise UrlRejected(
                f"该地址解析到内网/保留地址（{host} → {ip}），出于安全考虑不允许抓取",
                "private_ip",
            )
    return ips


def check_url(url: str, check_dns: bool = True) -> str:
    """校验并返回规范化后的 URL；不通过抛 UrlRejected（reason 可直接展示给用户）"""
    url = normalize(url)
    try:
        u = urlparse(url)
    except Exception:
        raise UrlRejected("网址格式不正确", "bad_url")

    if u.scheme.lower() not in ALLOWED_SCHEMES:
        raise UrlRejected("仅支持 http/https 协议的网址", "bad_scheme")
    if not u.hostname:
        raise UrlRejected("网址缺少域名部分", "no_host")

    port = u.port or (443 if u.scheme.lower() == "https" else 80)
    if port not in allowed_ports():
        raise UrlRejected(
            f"该地址使用了非 Web 端口 {port}，只允许 {sorted(allowed_ports())}",
            "bad_port",
        )

    # 策略开关：默认 open，不限制域名；只有 demo/allowlist 才查站点清单
    pol = policy()
    if pol == "demo":
        if not _host_matches(u.hostname, demo_hosts()):
            raise UrlRejected(
                f"当前是演示模式，仅允许 {sorted(demo_hosts())}；"
                "如需抓取其他站点，把 .env 里 PAGE_URL_POLICY 改成 open",
                "policy_demo",
            )
    elif pol == "allowlist":
        if not _host_matches(u.hostname, allowlist_hosts()):
            raise UrlRejected(
                "当前是白名单模式，该域名不在 PAGE_URL_ALLOWLIST 里",
                "policy_allowlist",
            )

    if check_dns:
        resolve_and_check_host(u.hostname)
    return url


def check_redirect(final_url: str):
    """爬虫跟随 302 之后，对落地地址再做一次同样校验。

    不做这一步，前面所有校验都能被绕过：公网短链 302 到 169.254.169.254 即可。
    """
    check_url(final_url)
    return True
