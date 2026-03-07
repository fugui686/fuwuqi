# -*- coding: utf-8 -*-

import os
import time
import argparse
import threading
import requests  # ✅ 新增：用于探活/发TG提醒

import retention_core as core
from retention_bot import TelegramBot


# =========================
# ✅ 智能模式鉴权探活 + 超管私聊提醒（401/403/CF）
# =========================
def _tg_send_to_super_admins(cfg, text):
    """使用 config.ini 的 bot_token + super_admin_ids 私聊提醒"""
    bot_token = (getattr(cfg, "bot_token", "") or "").strip()
    admins_raw = (getattr(cfg, "super_admin_ids", "") or "").strip()
    if not bot_token or not admins_raw:
        return

    ids = []
    for part in admins_raw.replace("，", ",").replace(" ", ",").split(","):
        part = part.strip()
        if part.isdigit():
            ids.append(part)
    if not ids:
        return

    url = "https://api.telegram.org/bot%s/sendMessage" % bot_token
    for uid in ids:
        try:
            requests.post(
                url,
                data={"chat_id": uid, "text": text, "disable_web_page_preview": "true"},
                timeout=20,
            )
        except Exception:
            pass


def _should_notify_cooldown(tag, cooldown_seconds=1800):
    """冷却：避免连续提醒刷屏。tag 用于区分不同类型提醒"""
    path = os.path.join(core.app_dir(), "notify_%s.ts" % tag)
    now = int(time.time())
    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                last = int((f.read() or "0").strip() or "0")
            if now - last < cooldown_seconds:
                return False
    except Exception:
        pass

    try:
        with open(path, "w", encoding="utf-8") as f:
            f.write(str(now))
    except Exception:
        pass
    return True


def _probe_auth_ok(cfg, alias_map):
    """
    探活：用当前 access_token + cookie 调一个轻量接口，
    判断是否 401/403 或 Cloudflare/WAF HTML 拦截。
    返回 (ok, reason)
    """
    try:
        child = None
        try:
            for k in sorted(alias_map.keys()):
                child = alias_map[k]
                break
        except Exception:
            child = None
        if not child:
            child = "0"

        headers = core.get_headers(
            cfg.平台ID,
            child,
            cfg.ht,
            cfg.token,
            access_token=getattr(cfg, "access_token", ""),
        )

        url = "https://%s/api/go-gateway-internal/user/advancedGetUserListV2" % cfg.ht
        payload = {"current": 1, "size": 1, "childSiteCode": child}

        r = requests.post(url, headers=headers, json=payload, timeout=25)
        ct = (r.headers.get("content-type") or "").lower()
        txt_head = (r.text or "")[:200]

        if r.status_code in (401, 403):
            return False, "HTTP %s" % r.status_code

        if ("text/html" in ct) or ("CLOUDFLARE" in txt_head.upper()):
            return False, "可能被风控/CF拦截（HTML响应）"

        if "application/json" in ct:
            try:
                js = r.json()
            except Exception:
                return False, "JSON解析失败"
            msg = str(js.get("msg") or js.get("message") or "")
            if ("unauthor" in msg.lower()) or ("expired" in msg.lower()):
                return False, msg[:80]

        return True, "OK"

    except Exception as e:
        return False, "探活异常：%s" % e


class SmartScheduler(object):
    def __init__(self, interval_minutes=30):
        self.interval = max(1, int(interval_minutes)) * 60
        self._lock = threading.Lock()
        self._running_task = False

    def run_forever(self):
        core.log("智能模式启动：每 %d 分钟触发一次“全站点每日更新”（运行中则跳过）" % (self.interval // 60))
        while True:
            try:
                if self._try_start_task():
                    try:
                        core.ensure_alias_map_file()
                        cfg = core.load_global_config_from_ini()
                        alias_map = core.load_alias_map()

                        # ✅ 先探活鉴权
                        ok, reason = _probe_auth_ok(cfg, alias_map)
                        if not ok:
                            core.log("[智能模式] 探活失败：%s" % reason)
                            if _should_notify_cooldown("auth_fail", cooldown_seconds=1800):
                                _tg_send_to_super_admins(
                                    cfg,
                                    "⚠️ 智能模式鉴权可能失效：%s\n"
                                    "已自动暂停智能模式（避免持续触发风控）。\n"
                                    "请私聊机器人发送：/6位验证码（例如 /596695）更新令牌，"
                                    "然后再私聊：/开启智能模式 重新启动。" % reason,
                                )
                            # ✅ 探活失败：直接退出 smart 进程（暂停智能模式）
                            return

                        # ✅ 鉴权OK再执行每日更新
                        msg = core.每日更新_全站点(cfg, alias_map)
                        core.log(msg)

                    finally:
                        self._finish_task()
                else:
                    core.log("智能模式：检测到任务正在运行，本次跳过")
            except Exception as e:
                core.log("[智能模式错误] %s" % e)

            time.sleep(self.interval)

    def _try_start_task(self):
        with self._lock:
            if self._running_task:
                return False
            self._running_task = True
            return True

    def _finish_task(self):
        with self._lock:
            self._running_task = False


def parse_date(s):
    return core.datetime.strptime(s, "%Y-%m-%d").date()


def main():
    os.chdir(core.app_dir())
    core.setup_logging()
    core.ensure_alias_map_file()

    parser = argparse.ArgumentParser(description="RetentionTool 无UI服务器版（Python3.6兼容，拆分版）")
    parser.add_argument("--mode", choices=["daily", "history", "cleanup", "bot", "smart"], required=True)
    parser.add_argument("--start", help="history: start YYYY-MM-DD")
    parser.add_argument("--end", help="history: end YYYY-MM-DD")
    parser.add_argument("--months", type=int, default=6, help="cleanup: keep N months")
    parser.add_argument("--interval", type=int, default=30, help="smart: interval minutes")

    args = parser.parse_args()

    cfg = core.load_global_config_from_ini()
    alias_map = core.load_alias_map()

    if args.mode == "daily":
        msg = core.每日更新_全站点(cfg, alias_map)
        core.log(msg)
        print(msg)
        return

    if args.mode == "history":
        if not args.start or not args.end:
            raise SystemExit("history 模式必须提供 --start 与 --end")
        start_d = parse_date(args.start)
        end_d = parse_date(args.end)
        msg = core.补历史_全站点(cfg, alias_map, start_d, end_d)
        core.log(msg)
        print(msg)
        return

    if args.mode == "cleanup":
        msg = core.cleanup_all_sites(cfg, alias_map, args.months)
        core.log(msg)
        print(msg)
        return

    if args.mode == "bot":
        bot = TelegramBot(cfg)
        bot.start()
        while True:
            time.sleep(60)

    if args.mode == "smart":
        sch = SmartScheduler(args.interval)
        sch.run_forever()


if __name__ == "__main__":
    main()
