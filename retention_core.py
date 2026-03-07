# -*- coding: utf-8 -*-
import os
import sys
import re
import csv
import time
import random
import shutil
import logging
import requests
import configparser
import pytz
from datetime import datetime, date, timedelta

BJ_TZ = pytz.timezone("Asia/Shanghai")

DATA_DIR_NAME = "data"
ALIAS_MAP_FILE = "子平台别名映射.txt"

FIRST_FILE = "first_deposit.csv"
RECHARGE_FILE = "daily_recharge.csv"
REGISTER_FILE = "register.csv"
VISIT_FILE = "daily_visit.csv"
LOGIN_FILE = "daily_login.csv"

MONTH_DIR_FMT_LEN = 7  # YYYY-MM


def is_frozen():
    return getattr(sys, "frozen", False)


def app_dir():
    if is_frozen():
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))


def setup_logging():
    os.makedirs(app_dir(), exist_ok=True)
    log_path = os.path.join(app_dir(), "retention_server.log")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(log_path, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )
    logging.info("日志初始化完成：%s", log_path)


def log(msg):
    logging.info(msg)


def user_config_dir():
    if sys.platform.startswith("win"):
        base = os.environ.get("APPDATA", os.path.expanduser("~"))
        return os.path.join(base, "RetentionTool")
    base = os.path.join(os.path.expanduser("~"), ".config")
    return os.path.join(base, "retention_tool")


def is_dir_writable(d):
    try:
        os.makedirs(d, exist_ok=True)
        testfile = os.path.join(d, ".write_test.tmp")
        with open(testfile, "w", encoding="utf-8") as f:
            f.write("ok")
        os.remove(testfile)
        return True
    except Exception:
        return False


def resolve_config_path_for_load():
    p1 = os.path.join(app_dir(), "config.ini")
    if os.path.exists(p1):
        return p1
    return os.path.join(user_config_dir(), "config.ini")


def resolve_config_path_for_save():
    d1 = app_dir()
    if is_dir_writable(d1):
        return os.path.join(d1, "config.ini")
    d2 = user_config_dir()
    os.makedirs(d2, exist_ok=True)
    return os.path.join(d2, "config.ini")


def save_ini_defaults(updates):
    p_load = resolve_config_path_for_load()
    p_save = resolve_config_path_for_save()
    cp = configparser.ConfigParser()
    if os.path.exists(p_load):
        cp.read(p_load, encoding="utf-8")
    if "DEFAULT" not in cp:
        cp["DEFAULT"] = {}
    for k, v in (updates or {}).items():
        cp["DEFAULT"][k] = (v or "").strip()
    with open(p_save, "w", encoding="utf-8") as f:
        cp.write(f)
    return p_save


def ensure_alias_map_file():
    path = os.path.join(app_dir(), ALIAS_MAP_FILE)
    if os.path.exists(path):
        return
    content = (
        "# 子平台别名映射文件\n"
        "# 格式：alias=child_id，例如：B01=2610\n"
        "# child_id=0 会被忽略\n"
        "B01=2610\n"
        "B02=2706\n"
        "B03=3006\n"
    )
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)
    log("[提示] 未找到 %s，已自动生成：%s" % (ALIAS_MAP_FILE, path))


def load_alias_map():
    path = os.path.join(app_dir(), ALIAS_MAP_FILE)
    mp = {}
    if not os.path.exists(path):
        return mp
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            if "=" not in s:
                continue
            k, v = s.split("=", 1)
            k = k.strip().upper()
            v = v.strip()
            if not k or not v:
                continue
            if v in ("0", "0000"):
                continue
            mp[k] = v
    return mp


def read_csv(path):
    if not os.path.exists(path):
        return []
    for enc in ("utf-8-sig", "utf-8", "gbk", "cp936"):
        try:
            with open(path, "r", encoding=enc, newline="") as f:
                return list(csv.DictReader(f))
        except UnicodeDecodeError:
            continue
        except Exception:
            break
    with open(path, "r", encoding="utf-8", errors="replace", newline="") as f:
        return list(csv.DictReader(f))


def write_csv(path, fieldnames, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def append_csv(path, fieldnames, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    file_exists = os.path.exists(path)
    with open(path, "a", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            w.writeheader()
        for r in rows:
            w.writerow(r)


def is_month_key(s):
    s = (s or "").strip()
    if len(s) != MONTH_DIR_FMT_LEN or s[4] != "-":
        return False
    y, m = s.split("-", 1)
    if not (y.isdigit() and m.isdigit()):
        return False
    mi = int(m)
    return 1 <= mi <= 12


def month_key_from_date(d):
    return d.strftime("%Y-%m")


def site_root_dir(alias):
    d = os.path.join(app_dir(), DATA_DIR_NAME, alias.upper())
    os.makedirs(d, exist_ok=True)
    return d


def ensure_site_month_dir(alias, month_key):
    root = site_root_dir(alias)
    mdir = os.path.join(root, month_key)
    os.makedirs(mdir, exist_ok=True)
    return mdir


def site_month_file(alias, month_key, filename):
    return os.path.join(ensure_site_month_dir(alias, month_key), filename)


def list_site_month_dirs(alias):
    root = site_root_dir(alias)
    try:
        names = os.listdir(root)
    except Exception:
        return []
    months = [n for n in names if os.path.isdir(os.path.join(root, n)) and is_month_key(n)]
    months.sort()
    return months


def norm(s):
    return str(s or "").strip().lower()


def safe_int(x):
    try:
        return int(float(x))
    except Exception:
        return 0


def safe_float(x):
    try:
        return float(x)
    except Exception:
        return 0.0


def unique_user_key(site_alias, user_id):
    return "%s::%s" % (site_alias.upper(), str(user_id or "").strip())


def month_of_date_str(d_str):
    return (d_str or "")[:7]


def day_ts_range(d):
    start = BJ_TZ.localize(datetime(d.year, d.month, d.day, 0, 0, 0))
    end = BJ_TZ.localize(datetime(d.year, d.month, d.day, 23, 59, 59))
    return int(start.timestamp()), int(end.timestamp())


def is_date_str(s):
    try:
        datetime.strptime(s, "%Y-%m-%d")
        return True
    except Exception:
        return False


def title_scope_all():
    return "组合：全部平台（所有渠道）"


def title_scope_site():
    return "组合：指定平台（所有渠道）"


def title_scope_channel(channel):
    return "渠道：%s" % channel


class GlobalConfig(object):
    def __init__(
        self,
        平台ID="",
        ht="",
        token="",
        bot_token="",
        chat_id="",
        super_admin_ids="",
        admin_ids="",
        last_finalize_date="",
        access_token="",
    ):
        self.平台ID = (平台ID or "").strip()
        self.ht = self.normalize_ht(ht)
        self.token = (token or "").strip()
        self.bot_token = (bot_token or "").strip()
        self.chat_id = (chat_id or "").strip()
        self.super_admin_ids = (super_admin_ids or "").strip()
        self.admin_ids = (admin_ids or "").strip()
        self.last_finalize_date = (last_finalize_date or "").strip()
        self.access_token = (access_token or "").strip()

    @staticmethod
    def normalize_ht(ht):
        ht_norm = (ht or "").strip()
        if ht_norm.startswith("https://"):
            ht_norm = ht_norm[len("https://") :]
        if ht_norm.startswith("http://"):
            ht_norm = ht_norm[len("http://") :]
        return ht_norm.rstrip("/")


class SiteConfig(object):
    def __init__(self, alias, 子平台ID, 平台ID, ht, token, access_token=""):
        self.alias = (alias or "").strip().upper()
        self.子平台ID = (子平台ID or "").strip()
        self.平台ID = (平台ID or "").strip()
        self.ht = (ht or "").strip()
        self.token = (token or "").strip()
        self.access_token = (access_token or "").strip()

    def month_first_csv(self, month_key):
        return site_month_file(self.alias, month_key, FIRST_FILE)

    def month_register_csv(self, month_key):
        return site_month_file(self.alias, month_key, REGISTER_FILE)

    def month_login_csv(self, month_key):
        return site_month_file(self.alias, month_key, LOGIN_FILE)

    def month_recharge_csv(self, month_key):
        return site_month_file(self.alias, month_key, RECHARGE_FILE)

    def month_visit_csv(self, month_key):
        return site_month_file(self.alias, month_key, VISIT_FILE)


def build_sites(global_cfg, alias_map):
    sites = []
    for alias, child_id in (alias_map or {}).items():
        sites.append(
            SiteConfig(
                alias=alias,
                子平台ID=child_id,
                平台ID=global_cfg.平台ID,
                ht=global_cfg.ht,
                token=global_cfg.token,
                access_token=getattr(global_cfg, "access_token", "") or "",
            )
        )
    return sites


def load_global_config_from_ini():
    p = resolve_config_path_for_load()
    cp = configparser.ConfigParser()
    if os.path.exists(p):
        cp.read(p, encoding="utf-8")
    d = cp["DEFAULT"] if "DEFAULT" in cp else {}

    def _get(key, default=""):
        try:
            return (d.get(key, default) or "").strip()
        except Exception:
            return default

    cfg = GlobalConfig(
        平台ID=_get("platform_id", ""),
        ht=_get("ht", ""),
        token=_get("token", ""),
        bot_token=_get("bot_token", ""),
        chat_id=_get("chat_id", "") or _get("group_whitelist", ""),
        super_admin_ids=_get("super_admin_ids", ""),
        admin_ids=_get("admin_ids", ""),
        last_finalize_date=_get("last_finalize_date", ""),
        access_token=_get("access_token", ""),
    )
    if os.path.exists(p):
        log("已加载配置：%s" % p)
    return cfg


# ✅ 关键对齐：永远带 Cookie + Bearer
def get_headers(平台ID, 子平台ID, ht, token, access_token=None):
    if not ht or not 平台ID:
        raise RuntimeError("配置缺失：请先填写 ht、platform_id")

    h = {
        "accept": "application/json, text/plain, */*",
        "accept-language": "zh-CN,zh;q=0.9",
        "childsitecode": 子平台ID or "0",
        "companycode": 平台ID,
        "content-type": "application/json",
        "loginbacktype": "3",
        "sitecode": 平台ID,
    }

    if token:
        h["cookie"] = token.strip()

    at = (access_token or "").strip()
    if at:
        h["Authorization"] = "Bearer %s" % at

    return h


def _sleep_backoff(attempt, reason, base=0.8, cap=8.0):
    delay = min(cap, base * (2 ** (attempt - 1)))
    jitter = random.uniform(0.7, 1.4)
    sleep_s = max(0.2, delay * jitter)
    log("[退避] %s，第%d次重试：sleep %.2fs" % (reason, attempt, sleep_s))
    time.sleep(sleep_s)


def post_json_with_retry(url, headers, payload, retries=2, timeout=25):
    last_text = ""
    for attempt in range(1, retries + 2):
        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=timeout)
            status = resp.status_code
            last_text = (resp.text or "")[:300]

            if status != 200:
                if status == 429 or (500 <= status <= 599):
                    if attempt <= retries:
                        _sleep_backoff(attempt, "HTTP %d" % status)
                        continue
                log("[HTTP %d] %s" % (status, url))
                if last_text:
                    log(last_text)
                return None

            try:
                js = resp.json()
            except Exception as e:
                if attempt <= retries:
                    _sleep_backoff(attempt, "JSON解析失败: %s" % e)
                    continue
                log("[错误] JSON解析失败: %s" % e)
                if last_text:
                    log(last_text)
                return None

            if not isinstance(js, dict):
                log("[错误] 返回非 JSON dict")
                return None
            return js

        except requests.RequestException as e:
            if attempt <= retries:
                _sleep_backoff(attempt, "请求异常: %s" % e)
                continue
            log("[异常] 请求失败：%s" % e)
            return None
    return None


def _extract_list(js):
    if not js or not isinstance(js, dict):
        return []
    data = js.get("data")
    if isinstance(data, dict):
        lst = data.get("data") or data.get("list") or data.get("rows")
        if isinstance(lst, list):
            return lst
    if isinstance(data, list):
        return data
    return []


# -------- Fetch: 首存（旧版接口路径）--------
def fetch_first_deposit_for_day(cfg, d):
    start_ts, end_ts = day_ts_range(d)
    url = "https://%s/api/go-gateway-internal/user/advancedGetUserListV2" % cfg.ht
    all_rows = []
    page, size = 1, 1000
    d_str = d.strftime("%Y-%m-%d")

    headers = get_headers(cfg.平台ID, cfg.子平台ID, cfg.ht, cfg.token, access_token=getattr(cfg, "access_token", ""))

    while True:
        payload = {
            "selectTimeKey": 2,
            "accountTypes": [],
            "current": page,
            "size": size,
            "firstPayTimeFrom": start_ts,
            "firstPayTimeTo": end_ts,
            "childSiteCode": cfg.子平台ID or "0",
        }
        js = post_json_with_retry(url, headers, payload)
        if not js:
            log("[%s][首存] %s 第%d页 获取失败" % (cfg.alias, d_str, page))
            break

        data = _extract_list(js)
        if not data:
            break

        for item in data:
            user_id = item.get("useridx") or item.get("userIdx")
            if not user_id:
                continue
            first_amount_raw = item.get("firstPayAmount")
            try:
                first_amount = float(first_amount_raw) if first_amount_raw is not None else 0.0
            except Exception:
                first_amount = 0.0
            if first_amount <= 0:
                continue
            channel = (item.get("regpkgidName") or "").strip()
            all_rows.append({"user_id": str(user_id), "first_date": d_str, "first_amount": first_amount, "channel": channel})

        if len(data) < size:
            break
        page += 1

    log("[%s][首存] %s 获取到 %d 条记录" % (cfg.alias, d_str, len(all_rows)))
    return all_rows


# -------- Fetch: 注册（旧版接口路径）--------
def fetch_register_for_day(cfg, d):
    start_ts, end_ts = day_ts_range(d)
    url = "https://%s/api/go-gateway-internal/user/advancedGetUserListV2" % cfg.ht
    all_rows = []
    page, size = 1, 1000
    d_str = d.strftime("%Y-%m-%d")

    headers = get_headers(cfg.平台ID, cfg.子平台ID, cfg.ht, cfg.token, access_token=getattr(cfg, "access_token", ""))

    while True:
        payload = {
            "selectTimeKey": 0,
            "accountTypes": [],
            "current": page,
            "size": size,
            "registerTimeFrom": start_ts,
            "registerTimeTo": end_ts,
            "childSiteCode": cfg.子平台ID or "0",
        }
        js = post_json_with_retry(url, headers, payload)
        if not js:
            log("[%s][注册] %s 第%d页 获取失败" % (cfg.alias, d_str, page))
            break

        data = _extract_list(js)
        if not data:
            break

        for item in data:
            user_id = item.get("useridx") or item.get("userIdx")
            if not user_id:
                continue
            channel = (item.get("regpkgidName") or "").strip()
            all_rows.append({"user_id": str(user_id), "reg_date": d_str, "channel": channel})

        if len(data) < size:
            break
        page += 1

    log("[%s][注册] %s 获取到 %d 条记录" % (cfg.alias, d_str, len(all_rows)))
    return all_rows


def fetch_login_for_day(cfg, d):
    start_ts, end_ts = day_ts_range(d)
    url = "https://%s/api/go-gateway-internal/user/getUserActionLogs" % cfg.ht
    all_rows = []
    seen_users = set()
    page, size = 1, 1000
    d_str = d.strftime("%Y-%m-%d")

    headers = get_headers(cfg.平台ID, cfg.子平台ID, cfg.ht, cfg.token, access_token=getattr(cfg, "access_token", ""))

    while True:
        payload = {
            "childSiteCode": cfg.子平台ID or "0",
            "timeStart": start_ts,
            "timeEnd": end_ts,
            "operationType": 1,
            "actionItems": 1,
            "name": 25,
            "result": "0",
            "current": page,
            "size": size,
        }
        js = post_json_with_retry(url, headers, payload)
        if not js:
            log("[%s][登录] %s 第%d页 获取失败" % (cfg.alias, d_str, page))
            break

        data = _extract_list(js)
        if not data:
            break

        for item in data:
            user_id = item.get("useridx") or item.get("userIdx")
            if not user_id:
                continue
            uid_str = str(user_id)
            if uid_str in seen_users:
                continue
            seen_users.add(uid_str)
            all_rows.append({"user_id": uid_str, "login_date": d_str})

        if len(data) < size:
            break
        page += 1

    log("[%s][登录] %s 获取到 %d 条记录（去重后）" % (cfg.alias, d_str, len(all_rows)))
    return all_rows


def fetch_member_report_for_day(cfg, d):
    start_ts, end_ts = day_ts_range(d)
    url = "https://%s/api/go-gateway-internal/noEncrypt/statistics/report/user_report" % cfg.ht
    all_rows = []
    page, size = 1, 1000
    d_str = d.strftime("%Y-%m-%d")

    headers = get_headers(cfg.平台ID, cfg.子平台ID, cfg.ht, cfg.token, access_token=getattr(cfg, "access_token", ""))

    while True:
        payload = {
            "currency": "CNY",
            "startTime": start_ts,
            "endTime": end_ts,
            "childSiteCode": cfg.子平台ID or "0",
            "pageSort": {"page": page, "limit": size},
        }
        js = post_json_with_retry(url, headers, payload)
        if not js:
            log("[%s][会员报表] %s 第%d页 获取失败" % (cfg.alias, d_str, page))
            break

        data = _extract_list(js)
        if not data:
            break

        for item in data:
            user_id = item.get("userIdx") or item.get("useridx")
            if not user_id:
                continue
            try:
                deposit = float(item.get("deposit", 0) or 0)
            except Exception:
                deposit = 0.0
            try:
                withdraw_amount = float(item.get("withdraw", 0) or 0)
            except Exception:
                withdraw_amount = 0.0

            if deposit > 0 or withdraw_amount > 0:
                all_rows.append({"user_id": str(user_id), "pay_date": d_str, "pay_amount": deposit, "withdraw_amount": withdraw_amount})

        if len(data) < size:
            break
        page += 1

    log("[%s][会员报表] %s 获取到 %d 条记录" % (cfg.alias, d_str, len(all_rows)))
    return all_rows


def fetch_visit_for_day(cfg, d):
    start_ts, end_ts = day_ts_range(d)
    url = "https://%s/api/go-gateway-internal/noEncrypt/statistics/channel/channel_download_report" % cfg.ht
    d_str = d.strftime("%Y-%m-%d")

    headers = get_headers(cfg.平台ID, cfg.子平台ID, cfg.ht, cfg.token, access_token=getattr(cfg, "access_token", ""))

    page, size = 1, 1000
    visits_by_channel = {}

    while True:
        payload = {
            "childSiteCode": cfg.子平台ID or "0",
            "valField": "visits",
            "startTime": start_ts,
            "endTime": end_ts,
            "pageSort": {"page": page, "limit": size},
        }
        js = post_json_with_retry(url, headers, payload)
        if not js:
            log("[%s][访问量] %s 第%d页 获取失败" % (cfg.alias, d_str, page))
            break

        data = _extract_list(js)
        if not data:
            break

        for item in data:
            channel = (item.get("channelName") or "").strip()
            if not channel:
                continue
            try:
                visits = float(item.get("visits", 0) or 0)
            except Exception:
                visits = 0.0
            visits_by_channel[channel] = visits_by_channel.get(channel, 0.0) + visits

        if len(data) < size:
            break
        page += 1

    rows = [{"channel": ch, "visit_date": d_str, "visit_count": cnt} for ch, cnt in visits_by_channel.items()]
    log("[%s][访问量] %s 获取到 %d 条记录" % (cfg.alias, d_str, len(rows)))
    return rows


# -------------------- Save month CSVs --------------------
def save_first_deposit_month(cfg, month_key, new_rows):
    if not new_rows:
        return
    path = cfg.month_first_csv(month_key)
    fieldnames = ["user_id", "first_date", "first_amount", "channel"]
    exist = read_csv(path)
    by_user = {r["user_id"]: r for r in exist if r.get("user_id")}
    for r in new_rows:
        uid = r["user_id"]
        by_user[uid] = r
    write_csv(path, fieldnames, list(by_user.values()))


def save_register_month(cfg, month_key, new_rows):
    if not new_rows:
        return
    path = cfg.month_register_csv(month_key)
    fieldnames = ["user_id", "reg_date", "channel"]
    exist = read_csv(path)
    by_user = {r["user_id"]: r for r in exist if r.get("user_id")}
    for r in new_rows:
        uid = r["user_id"]
        by_user[uid] = r
    write_csv(path, fieldnames, list(by_user.values()))


def save_login_month(cfg, month_key, new_rows):
    if not new_rows:
        return
    path = cfg.month_login_csv(month_key)
    fieldnames = ["user_id", "login_date"]
    exist = read_csv(path)
    seen = set((r.get("user_id", ""), r.get("login_date", "")) for r in exist)
    to_write = []
    for r in new_rows:
        key = (r.get("user_id", ""), r.get("login_date", ""))
        if key in seen:
            continue
        seen.add(key)
        to_write.append({"user_id": str(r.get("user_id", "")), "login_date": str(r.get("login_date", ""))})
    if to_write:
        append_csv(path, fieldnames, to_write)


def save_member_report_month(cfg, month_key, new_rows):
    if not new_rows:
        return
    path = cfg.month_recharge_csv(month_key)
    fieldnames = ["user_id", "pay_date", "pay_amount", "withdraw_amount"]
    exist = read_csv(path)
    by_key = {}
    for r in exist:
        uid = (r.get("user_id") or "").strip()
        ds = (r.get("pay_date") or "").strip()
        if uid and ds:
            by_key[(uid, ds)] = r
    for r in new_rows:
        uid = (r.get("user_id") or "").strip()
        ds = (r.get("pay_date") or "").strip()
        if uid and ds:
            by_key[(uid, ds)] = r
    write_csv(path, fieldnames, list(by_key.values()))


def save_visit_month(cfg, month_key, new_rows):
    if not new_rows:
        return
    path = cfg.month_visit_csv(month_key)
    fieldnames = ["channel", "visit_date", "visit_count"]
    exist = read_csv(path)
    by_key = {}
    for r in exist:
        ch = (r.get("channel") or "").strip()
        ds = (r.get("visit_date") or "").strip()
        if ch and ds:
            by_key[(ch.lower(), ds)] = r
    for r in new_rows:
        ch = (r.get("channel") or "").strip()
        ds = (r.get("visit_date") or "").strip()
        if ch and ds:
            by_key[(ch.lower(), ds)] = r
    write_csv(path, fieldnames, list(by_key.values()))


def 每日更新_单站点(cfg, include_yesterday=True):
    today = date.today()
    yesterday = today - timedelta(days=1)
    dates = (yesterday, today) if include_yesterday else (today,)
    if include_yesterday:
        log("=== [%s] 每日更新: 处理昨日 %s + 今日 %s ===" % (cfg.alias, yesterday, today))
    else:
        log("=== [%s] 每日更新: 已结算昨日，仅处理今日 %s ===" % (cfg.alias, today))

    for d in dates:
        mkey = month_key_from_date(d)
        save_register_month(cfg, mkey, fetch_register_for_day(cfg, d))
        save_login_month(cfg, mkey, fetch_login_for_day(cfg, d))
        save_first_deposit_month(cfg, mkey, fetch_first_deposit_for_day(cfg, d))
        save_member_report_month(cfg, mkey, fetch_member_report_for_day(cfg, d))
        save_visit_month(cfg, mkey, fetch_visit_for_day(cfg, d))

    return "[%s] 每日更新完成：%s" % (cfg.alias, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))


def 每日更新_全站点(global_cfg, alias_map):
    sites = build_sites(global_cfg, alias_map)
    if not sites:
        raise RuntimeError("映射文件为空/无有效站点：%s" % ALIAS_MAP_FILE)

    today_str = date.today().strftime("%Y-%m-%d")
    last_done = (global_cfg.last_finalize_date or "").strip()
    include_yesterday = (last_done != today_str)

    if include_yesterday:
        log("[每日更新优化] 今日首次运行：将结算昨日一次（last_finalize_date=%s -> %s）" % (last_done or "空", today_str))
    else:
        log("[每日更新优化] 今日已结算昨日（last_finalize_date=%s），本次仅抓今日数据" % last_done)

    msgs = []
    for s in sites:
        msgs.append(每日更新_单站点(s, include_yesterday=include_yesterday))

    if include_yesterday:
        global_cfg.last_finalize_date = today_str
        p = save_ini_defaults({"last_finalize_date": today_str})
        log("[每日更新优化] 已写入 last_finalize_date=%s 到配置：%s" % (today_str, p))

    return "全站点每日更新完成：\n" + "\n".join(msgs)


def _filter_month_csv_by_date_range(path, date_field, start_str, end_str):
    if not os.path.exists(path):
        return 0
    rows = read_csv(path)
    if not rows:
        return 0
    kept = []
    removed = 0
    for r in rows:
        ds = (r.get(date_field) or "").strip()
        if len(ds) == 10 and start_str <= ds <= end_str:
            removed += 1
            continue
        kept.append(r)
    if removed > 0:
        fieldnames = list(rows[0].keys())
        write_csv(path, fieldnames, kept)
    return removed


def 补历史_单站点(cfg, start_d, end_d):
    start_str = start_d.strftime("%Y-%m-%d")
    end_str = end_d.strftime("%Y-%m-%d")
    log("[%s] 补历史：%s ~ %s（按月文件删除区间后重抓）" % (cfg.alias, start_d, end_d))

    months = set()
    cur = start_d
    while cur <= end_d:
        months.add(month_key_from_date(cur))
        cur += timedelta(days=1)
    month_list = sorted(months)

    for mkey in month_list:
        _filter_month_csv_by_date_range(cfg.month_register_csv(mkey), "reg_date", start_str, end_str)
        _filter_month_csv_by_date_range(cfg.month_first_csv(mkey), "first_date", start_str, end_str)
        _filter_month_csv_by_date_range(cfg.month_recharge_csv(mkey), "pay_date", start_str, end_str)
        _filter_month_csv_by_date_range(cfg.month_visit_csv(mkey), "visit_date", start_str, end_str)
        _filter_month_csv_by_date_range(cfg.month_login_csv(mkey), "login_date", start_str, end_str)

    cur = start_d
    while cur <= end_d:
        mkey = month_key_from_date(cur)
        log("=== [%s] 补历史: 处理日期 %s ===" % (cfg.alias, cur))
        save_register_month(cfg, mkey, fetch_register_for_day(cfg, cur))
        save_login_month(cfg, mkey, fetch_login_for_day(cfg, cur))
        save_first_deposit_month(cfg, mkey, fetch_first_deposit_for_day(cfg, cur))
        save_member_report_month(cfg, mkey, fetch_member_report_for_day(cfg, cur))
        save_visit_month(cfg, mkey, fetch_visit_for_day(cfg, cur))
        cur += timedelta(days=1)

    return "[%s] 补历史完成：%s ~ %s" % (cfg.alias, start_d, end_d)


def 补历史_全站点(global_cfg, alias_map, start_d, end_d):
    sites = build_sites(global_cfg, alias_map)
    if not sites:
        raise RuntimeError("映射文件为空/无有效站点：%s" % ALIAS_MAP_FILE)
    msgs = []
    for s in sites:
        msgs.append(补历史_单站点(s, start_d, end_d))
    return "全站点补历史完成：\n" + "\n".join(msgs)


def first_day_of_month(d):
    return date(d.year, d.month, 1)


def add_months(d, months):
    y = d.year + (d.month - 1 + months) // 12
    m = (d.month - 1 + months) % 12 + 1
    return date(y, m, 1)


def calc_cutoff_month_key(months_to_keep, today=None):
    if today is None:
        today = date.today()
    if months_to_keep < 1:
        months_to_keep = 1
    this_month = first_day_of_month(today)
    earliest_month = add_months(this_month, -(months_to_keep - 1))
    return earliest_month.strftime("%Y-%m")


def cleanup_site_month_dirs(site_cfg, months_to_keep):
    cutoff_m = calc_cutoff_month_key(months_to_keep)
    months = list_site_month_dirs(site_cfg.alias)
    to_delete = [m for m in months if m < cutoff_m]
    deleted = []
    for m in to_delete:
        p = os.path.join(site_root_dir(site_cfg.alias), m)
        try:
            shutil.rmtree(p)
            deleted.append(m)
        except Exception as e:
            log("[%s][清理] 删除失败 %s：%s" % (site_cfg.alias, p, e))
    return cutoff_m, deleted


def cleanup_all_sites(global_cfg, alias_map, months_to_keep):
    sites = build_sites(global_cfg, alias_map)
    if not sites:
        raise RuntimeError("映射文件为空/无有效站点：%s" % ALIAS_MAP_FILE)
    cutoff_m = calc_cutoff_month_key(months_to_keep)
    total_deleted = 0
    for s in sites:
        _, deleted = cleanup_site_month_dirs(s, months_to_keep)
        total_deleted += len(deleted)
    return "清理完成：最早保留月份 %s，共删除 %d 个月份目录" % (cutoff_m, total_deleted)


# ===== 统计（保持你现有逻辑）=====
def calc_visit_register_first(scope_sites, target_date, channel_filter=None):
    chf = norm(channel_filter) if channel_filter else None
    mkey = month_of_date_str(target_date)

    total_visits = 0
    reg_users = set()
    first_users = set()

    for s in scope_sites:
        for r in read_csv(s.month_visit_csv(mkey)):
            if (r.get("visit_date") or "") != target_date:
                continue
            if chf and norm(r.get("channel")) != chf:
                continue
            total_visits += safe_int(r.get("visit_count", 0))

        for r in read_csv(s.month_register_csv(mkey)):
            if (r.get("reg_date") or "") != target_date:
                continue
            if chf and norm(r.get("channel")) != chf:
                continue
            uid = (r.get("user_id") or "").strip()
            if uid:
                reg_users.add(unique_user_key(s.alias, uid))

        for r in read_csv(s.month_first_csv(mkey)):
            if (r.get("first_date") or "") != target_date:
                continue
            if chf and norm(r.get("channel")) != chf:
                continue
            uid = (r.get("user_id") or "").strip()
            if uid:
                first_users.add(unique_user_key(s.alias, uid))

    return total_visits, len(reg_users), len(first_users)


def format_reply_convert(title, date_str, total_visits, total_reg, total_first):
    ratio = (total_first / float(total_reg) * 100.0) if total_reg > 0 else 0.0
    return "%s\n日期：%s\n总访问量：%d\n\n注册人数：%d\n首存人数：%d\n转换比例：%.2f%%" % (
        title, date_str, total_visits, total_reg, total_first, ratio
    )


def calc_amount_ranges(scope_sites, first_date, channel_filter=None):
    chf = norm(channel_filter) if channel_filter else None
    mkey = month_of_date_str(first_date)

    buckets = [
        ("10元", 10.0, 10.0),
        ("11~30元", 11.0, 30.0),
        ("31~50元", 31.0, 50.0),
        ("51~99元", 51.0, 99.0),
        ("100元以上", 100.0, float("inf")),
    ]

    amounts = {}
    for s in scope_sites:
        for r in read_csv(s.month_first_csv(mkey)):
            if (r.get("first_date") or "") != first_date:
                continue
            if chf and norm(r.get("channel")) != chf:
                continue
            uid = (r.get("user_id") or "").strip()
            if not uid:
                continue
            amt = safe_float(r.get("first_amount", 0))
            if amt <= 0:
                continue
            amounts[unique_user_key(s.alias, uid)] = amt

    total_users = len(amounts)
    total_amount = sum(amounts.values())

    res = []
    for name, lo, hi in buckets:
        cnt = 0
        amt_sum = 0.0
        for amt in amounts.values():
            if hi == float("inf"):
                if amt >= lo:
                    cnt += 1
                    amt_sum += amt
            else:
                if lo <= amt <= hi:
                    cnt += 1
                    amt_sum += amt
        cnt_pct = (cnt / float(total_users) * 100.0) if total_users > 0 else 0.0
        amt_pct = (amt_sum / float(total_amount) * 100.0) if total_amount > 0 else 0.0
        res.append({"name": name, "cnt": cnt, "amt": amt_sum, "cnt_pct": cnt_pct, "amt_pct": amt_pct})

    return {"total_users": total_users, "total_amount": total_amount, "ranges": res}


def format_reply_amount_ranges(title, first_date, stats):
    total_users = stats.get("total_users", 0)
    total_amount = stats.get("total_amount", 0.0)
    lines = [
        title,
        "首存日期：%s" % first_date,
        "总首存人数：%d" % int(total_users),
        "总首存金额：%d" % int(total_amount),
        "",
        "【金额区间统计】",
    ]
    for item in stats.get("ranges", []):
        lines.append("%s：" % item.get("name", ""))
        lines.append("    首存人数：%d" % int(item.get("cnt", 0)))
        lines.append("    首存金额：%d" % int(item.get("amt", 0)))
        lines.append("    人数占比：%.2f%%" % float(item.get("cnt_pct", 0.0)))
        lines.append("    金额占比：%.2f%%\n" % float(item.get("amt_pct", 0.0)))
    return "\n".join(lines)


def _earliest_date_in_scope_all_months(scope_sites, channel_filter=None):
    chf = norm(channel_filter) if channel_filter else None
    dates = []

    for s in scope_sites:
        for m in list_site_month_dirs(s.alias):
            # 首存日期（带 channel）
            for r in read_csv(s.month_first_csv(m)):
                ds = (r.get("first_date") or "").strip()
                if len(ds) != 10:
                    continue
                if chf and norm(r.get("channel")) != chf:
                    continue
                dates.append(ds)

    return min(dates) if dates else ""

def _build_user_channel_map(scope_sites, month_list):
    """
    建立 user_id -> channel 的映射（用于渠道汇总时过滤充值/提现）
    优先用 register.csv 的 channel，其次用 first_deposit.csv 的 channel
    """
    user2ch = {}

    for s in scope_sites:
        for m in month_list:
            # register：更可靠
            for r in read_csv(s.month_register_csv(m)):
                uid = (r.get("user_id") or "").strip()
                ch = (r.get("channel") or "").strip()
                if not uid or not ch:
                    continue
                # 用首次出现的渠道，避免后面覆盖
                key = unique_user_key(s.alias, uid)
                if key not in user2ch:
                    user2ch[key] = ch

            # first：补漏
            for r in read_csv(s.month_first_csv(m)):
                uid = (r.get("user_id") or "").strip()
                ch = (r.get("channel") or "").strip()
                if not uid or not ch:
                    continue
                key = unique_user_key(s.alias, uid)
                if key not in user2ch:
                    user2ch[key] = ch

    return user2ch

def compute_summary(scope_sites, target_date=None, channel_filter=None):
    """
    汇总口径说明：
    - 访问量/注册/首存：按 target_date（如果传）过滤；且按 channel_filter（如果传）过滤
    - 充值/提现/盈亏/客单价：
        * 如果是 “渠道 + 日期” 查询（target_date 非空 且 channel_filter 非空）
          -> 只统计“当日首存会员(first_date=当天)”的充值/提现
        * 其它情况（全站/全时间/只日期无渠道等）保持原逻辑：统计符合条件的全量用户充值/提现
    """
    chf = norm(channel_filter) if channel_filter else None

    # 开始首存时间：按渠道过滤后的最早首存日期（用于展示）
    start_date = _earliest_date_in_scope_all_months(scope_sites, channel_filter=channel_filter) or (target_date or "")

    total_visits = 0
    reg_users = set()
    first_users = set()
    first_amount_total = 0.0
    total_recharge = 0.0
    total_withdraw = 0.0

    # 如果指定日期，就只查当月；否则查所有月
    if target_date:
        month_list_global = [month_of_date_str(target_date)]
    else:
        month_list_global = []
        for s in scope_sites:
            month_list_global.extend(list_site_month_dirs(s.alias))
        month_list_global = sorted(set(month_list_global))

    # ✅ 为“充值/提现”构建 user_id -> channel 映射（因为 daily_recharge.csv 没有 channel）
    user2ch = _build_user_channel_map(scope_sites, month_list_global)

    # ✅ 关键口径：渠道 + 日期 -> 仅统计当日首存 cohort 的充值/提现
    only_first_cohort = (target_date is not None) and (channel_filter is not None)

    # ========= 第1遍：统计访问/注册/首存，并构建 first_users（当日首存用户集合）=========
    for s in scope_sites:
        month_list = [month_of_date_str(target_date)] if target_date else list_site_month_dirs(s.alias)

        # 访问量：visit.csv 本身有 channel，可直接过滤
        for m in month_list:
            for r in read_csv(s.month_visit_csv(m)):
                ds = (r.get("visit_date") or "").strip()
                if target_date and ds != target_date:
                    continue
                if chf and norm(r.get("channel")) != chf:
                    continue
                total_visits += safe_int(r.get("visit_count", 0))

        # 注册：register.csv 有 channel，可直接过滤 + 记录用户集合
        for m in month_list:
            for r in read_csv(s.month_register_csv(m)):
                ds = (r.get("reg_date") or "").strip()
                if target_date and ds != target_date:
                    continue
                if chf and norm(r.get("channel")) != chf:
                    continue
                uid = (r.get("user_id") or "").strip()
                if uid:
                    reg_users.add(unique_user_key(s.alias, uid))

        # 首存：first_deposit.csv 有 channel，可直接过滤 + 首存金额（按“人”去重）
        for m in month_list:
            for r in read_csv(s.month_first_csv(m)):
                ds = (r.get("first_date") or "").strip()
                if target_date and ds != target_date:
                    continue
                if chf and norm(r.get("channel")) != chf:
                    continue
                uid = (r.get("user_id") or "").strip()
                if uid:
                    k = unique_user_key(s.alias, uid)
                    if k not in first_users:
                        first_users.add(k)
                        first_amount_total += safe_float(r.get("first_amount", 0))

    # ========= 第2遍：统计充值/提现（按口径过滤）=========
    for s in scope_sites:
        month_list = [month_of_date_str(target_date)] if target_date else list_site_month_dirs(s.alias)

        for m in month_list:
            for r in read_csv(s.month_recharge_csv(m)):
                ds = (r.get("pay_date") or "").strip()
                if target_date and ds != target_date:
                    continue

                uid = (r.get("user_id") or "").strip()
                if not uid:
                    continue

                k = unique_user_key(s.alias, uid)

                # 渠道过滤：使用 user2ch 映射
                if chf:
                    ch = user2ch.get(k)
                    if (not ch) or (norm(ch) != chf):
                        continue

                # ✅ 只统计“当日首存 cohort”
                if only_first_cohort:
                    if k not in first_users:
                        continue

                total_recharge += safe_float(r.get("pay_amount", 0))
                total_withdraw += safe_float(r.get("withdraw_amount", 0))

    reg_count = len(reg_users)
    first_count = len(first_users)
    reg_first_ratio = (first_count / float(reg_count) * 100.0) if reg_count > 0 else 0.0
    cash_profit = total_recharge - total_withdraw
    recharge_per_first = (total_recharge / float(first_count)) if first_count > 0 else 0.0
    recharge_per_reg = (total_recharge / float(reg_count)) if reg_count > 0 else 0.0

    return {
        "start_date": start_date,
        "visits": int(total_visits),
        "reg_count": reg_count,
        "first_count": first_count,
        "reg_first_ratio": reg_first_ratio,
        "first_amount_total": first_amount_total,
        "recharge_total": total_recharge,
        "withdraw_total": total_withdraw,
        "cash_profit": cash_profit,
        "recharge_per_first": recharge_per_first,
        "recharge_per_reg": recharge_per_reg,
    }

def format_reply_summary(title, target_date, summary):
    head = title
    if target_date:
        head += "\n日期：%s" % target_date
    return (
        "%s\n开始首存时间：%s\n访问量：%d\n\n注册人数：%d\n首存人数：%d\n首存金额：%d\n注册首存比例：%.2f%%\n\n总充值金额：%d\n总提现金额：%d\n现金盈亏：%d\n充值客单价：%.2f元\n注册客单价：%.2f元"
        % (
            head,
            summary.get("start_date") or "-",
            summary["visits"],
            summary["reg_count"],
            summary["first_count"],
            int(summary["first_amount_total"]),
            summary["reg_first_ratio"],
            int(summary["recharge_total"]),
            int(summary["withdraw_total"]),
            int(summary["cash_profit"]),
            summary["recharge_per_first"],
            summary["recharge_per_reg"],
        )
    )
