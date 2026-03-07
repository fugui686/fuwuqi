# -*- coding: utf-8 -*-
"""
半自动登录模块（Python 3.6 兼容）
- 你私聊发送 6 位 gaCode
- 登录成功后：
  1) 写入 access_token=
  2) 自动提取 Cookie 写入 token=
  3) 额外写 access_token_update_ts=
"""

import time
import requests
import retention_core as core

ym = 'ri52p630j.cg.ink'

LOGIN_URL = f"https://{ym}/auth/oauth/token"

# 这些 header 按你本地可用的来
FIXED_HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
    "appsystem": "Windows11",
    "browsertype": "Chrome",
    "companycode": "2610",
    "sitecode": "2610",
    "content-type": "application/json",
    "device": "90d306f5-e73e-4440-a2bd-855f98a1dd0c",
    "language": "zh",
    "loginbacktype": "3",
    "origin": f"https://{ym}",
    "referer": f"https://{ym}/",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
}

# 账号/密码/hash 你自己维护（按你本地能登录的那套）
FIXED_PAYLOAD = {
    "username": "qdlc888",
    "password": "d11713e1c0208fc92013af0eee455815cdf8b4387749ec467e868e0cc0fa9918",
    "randomStr": "83601772502309284",
    "code": "ewp5",
    "grant_type": "password",
    # gaCode 外部传入
}


def _cookiejar_to_str(cookiejar):
    """
    把 requests 的 CookieJar 转为 'k=v; k2=v2' 字符串
    """
    pairs = []
    try:
        for c in cookiejar:
            name = getattr(c, "name", None)
            value = getattr(c, "value", None)
            if not name:
                continue
            pairs.append("%s=%s" % (name, value if value is not None else ""))
    except Exception:
        return ""
    # 去重（后者覆盖前者）
    merged = {}
    for kv in pairs:
        if "=" not in kv:
            continue
        k, v = kv.split("=", 1)
        merged[k] = v
    return "; ".join(["%s=%s" % (k, merged[k]) for k in merged])


def login_with_gacode(ga_code):
    """
    使用 gaCode 登录获取 access_token + cookie_string
    成功返回 (access_token, cookie_string, raw_json)
    失败返回 (None, "", error_dict)
    """
    ga_code = (ga_code or "").strip()
    if (not ga_code) or (len(ga_code) != 6) or (not ga_code.isdigit()):
        return None, "", {"error": "gaCode 为空或格式不正确（应为6位数字）"}

    s = requests.Session()
    s.headers.update(dict(FIXED_HEADERS))

    payload = dict(FIXED_PAYLOAD)
    payload["gaCode"] = ga_code

    try:
        resp = s.post(LOGIN_URL, json=payload, timeout=25)
    except Exception as e:
        return None, "", {"error": "登录请求异常", "detail": str(e)}

    ct = (resp.headers.get("content-type") or "").lower()
    text = resp.text or ""

    if "application/json" not in ct:
        return None, "", {
            "error": "响应不是JSON",
            "status": resp.status_code,
            "content_type": ct,
            "text_head": text[:300],
        }

    try:
        js = resp.json()
    except Exception as e:
        return None, "", {
            "error": "JSON解析失败",
            "status": resp.status_code,
            "content_type": ct,
            "detail": str(e),
            "text_head": text[:300],
        }

    token = None
    if isinstance(js, dict):
        token = js.get("access_token")

    if not token:
        return None, "", js

    # ✅ 核心：把本次登录建立的 cookie 全部提取出来
    cookie_str = _cookiejar_to_str(s.cookies)

    # 有些场景 __cf_bm 这种在 resp.cookies 里也有，合并一次更稳
    try:
        resp_cookie_str = _cookiejar_to_str(resp.cookies)
        if resp_cookie_str:
            # 合并成 dict 再拼
            merged = {}
            for part in (cookie_str.split(";") if cookie_str else []):
                part = part.strip()
                if "=" in part:
                    k, v = part.split("=", 1)
                    merged[k.strip()] = v.strip()
            for part in (resp_cookie_str.split(";") if resp_cookie_str else []):
                part = part.strip()
                if "=" in part:
                    k, v = part.split("=", 1)
                    merged[k.strip()] = v.strip()
            cookie_str = "; ".join(["%s=%s" % (k, merged[k]) for k in merged])
    except Exception:
        pass

    # 补一个你旧 cookie 里常见的 isBrowserKeepAlive=1（没有也不影响）
    if cookie_str and ("isBrowserKeepAlive=" not in cookie_str):
        cookie_str = "isBrowserKeepAlive=1; " + cookie_str
    elif not cookie_str:
        cookie_str = "isBrowserKeepAlive=1"

    return token, cookie_str, js


def save_access_token_to_ini(access_token):
    """
    兼容你现有 retention_bot.py 的调用方式：
    - 以前只写 access_token
    - 现在：写 access_token + token(cookie)
    """
    token = (access_token or "").strip()
    # 这里没有 gaCode，所以不做登录；仅写 token 没意义
    # 正确调用：用下面的 save_login_result_to_ini()
    p = core.save_ini_defaults(
        {
            "access_token": token,
            "access_token_update_ts": str(int(time.time())),
        }
    )
    return p


def save_login_result_to_ini(access_token, cookie_str):
    """
    ✅ 推荐：登录成功后调用这个，写入 access_token + token(cookie)
    """
    token = (access_token or "").strip()
    cookie_str = (cookie_str or "").strip()

    p = core.save_ini_defaults(
        {
            "access_token": token,
            "token": cookie_str,  # ✅ 自动更新 cookie
            "access_token_update_ts": str(int(time.time())),
        }
    )
    return p
