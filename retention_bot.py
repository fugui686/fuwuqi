# -*- coding: utf-8 -*-
import os
import re
import time
import signal
import threading
import subprocess
import requests

import retention_core as core
import retention_auth


def parse_chat_id_list(raw):
    s = (raw or "").strip()
    if not s:
        return set()
    parts = re.split(r"[，,\s]+", s)
    out = set()
    for p in parts:
        p = (p or "").strip()
        if not p:
            continue
        if re.fullmatch(r"-?\d+", p):
            try:
                out.add(str(int(p)))
            except Exception:
                pass
    return out


def parse_user_id_list(raw):
    s = (raw or "").strip()
    if not s:
        return set()
    parts = re.split(r"[，,\s]+", s)
    out = set()
    for p in parts:
        p = (p or "").strip()
        if not p:
            continue
        if re.fullmatch(r"\d+", p):
            try:
                out.add(str(int(p)))
            except Exception:
                pass
    return out


def join_int_ids_str(ids_set):
    if not ids_set:
        return ""
    try:
        return ",".join(str(i) for i in sorted({int(x) for x in ids_set}))
    except Exception:
        return ",".join(sorted(ids_set))


def _is_date(s):
    return bool(re.fullmatch(r"\d{4}-\d{2}-\d{2}", (s or "").strip()))


class TelegramBot(object):
    def __init__(self, global_cfg):
        self.global_cfg = global_cfg
        self._running = False
        self._th = None

        self._lock = threading.Lock()
        self._allowed_chat_ids = parse_chat_id_list(
            getattr(global_cfg, "chat_id", "") or getattr(global_cfg, "group_whitelist", "")
        )
        self._super_admin_ids = parse_user_id_list(getattr(global_cfg, "super_admin_ids", ""))
        self._admin_ids = parse_user_id_list(getattr(global_cfg, "admin_ids", ""))

        self._bot_id = None
        self._bot_username = ""

        self._smart_pid_file = os.path.join(core.app_dir(), "smart.pid")

        self._history_lock = threading.Lock()
        self._history_running = False

    def start(self):
        if self._running:
            return
        if not self.global_cfg.bot_token:
            raise RuntimeError("bot_token 为空，无法启动 Telegram 机器人")
        self._running = True
        self._th = threading.Thread(target=self._run_loop, name="TelegramBot", daemon=True)
        self._th.start()
        core.log("[TG] Telegram 机器人已启动（无UI版）")

    def stop(self):
        self._running = False
        if self._th:
            self._th.join(timeout=3)

    def _is_super_admin(self, user_id):
        if user_id is None:
            return False
        try:
            uid = str(int(user_id))
        except Exception:
            return False
        with self._lock:
            return uid in self._super_admin_ids

    def _is_admin(self, user_id):
        if user_id is None:
            return False
        try:
            uid = str(int(user_id))
        except Exception:
            return False
        with self._lock:
            return (uid in self._super_admin_ids) or (uid in self._admin_ids)

    def _is_allowed_chat(self, chat_id_in):
        try:
            cid = str(int(chat_id_in))
        except Exception:
            return False
        with self._lock:
            return cid in self._allowed_chat_ids

    def _save_admin_ids(self):
        with self._lock:
            raw = join_int_ids_str(self._admin_ids)
        self.global_cfg.admin_ids = raw
        core.save_ini_defaults({"admin_ids": raw})
        return raw

    def _save_whitelist(self):
        with self._lock:
            raw = join_int_ids_str(self._allowed_chat_ids)
        self.global_cfg.chat_id = raw
        core.save_ini_defaults({"chat_id": raw, "group_whitelist": raw})
        return raw

    def _whitelist_add_chat(self, chat_id):
        cid = str(int(chat_id))
        with self._lock:
            if cid in self._allowed_chat_ids:
                return False, "群组已在白名单：%s" % cid
            self._allowed_chat_ids.add(cid)
        self._save_whitelist()
        return True, "✅ 已加白群组：%s" % cid

    def _whitelist_remove_chat(self, chat_id):
        cid = str(int(chat_id))
        with self._lock:
            if cid not in self._allowed_chat_ids:
                return False, "群组不在白名单：%s" % cid
            self._allowed_chat_ids.remove(cid)
        self._save_whitelist()
        return True, "✅ 已取消白名单：%s" % cid

    def _add_admin(self, user_id):
        uid = str(int(user_id))
        with self._lock:
            if uid in self._super_admin_ids:
                return False, "该用户是超级管理员，无需加入管理员：%s" % uid
            if uid in self._admin_ids:
                return False, "管理员已存在：%s" % uid
            self._admin_ids.add(uid)
        self._save_admin_ids()
        return True, "✅ 已添加管理员：%s" % uid

    def _remove_admin(self, user_id):
        uid = str(int(user_id))
        with self._lock:
            if uid in self._super_admin_ids:
                return False, "不能删除超级管理员：%s" % uid
            if uid not in self._admin_ids:
                return False, "管理员不存在：%s" % uid
            self._admin_ids.remove(uid)
        self._save_admin_ids()
        return True, "✅ 已删除管理员：%s" % uid

    def _format_admin_list(self):
        with self._lock:
            super_ids = join_int_ids_str(self._super_admin_ids)
            admin_ids = join_int_ids_str(self._admin_ids)
            chats = join_int_ids_str(self._allowed_chat_ids)
        return (
            "管理员：/管理员（列表）\n"
            "超管操作：/加管理员 123 /删管理员 123\n"
            "群组维护：/加群 -100xxx /删群 -100xxx\n"
            "参数示例：/加群 -1001234567890\n"
            "注意：/加管理员 里填的是个人 user_id\n\n"
            "【当前配置】\n"
            "super_admin_ids=%s\n"
            "admin_ids=%s\n"
            "chat_id(白名单)=%s"
        ) % (super_ids or "-", admin_ids or "-", chats or "-")

    def _read_pid(self):
        try:
            if not os.path.exists(self._smart_pid_file):
                return None
            with open(self._smart_pid_file, "r", encoding="utf-8") as f:
                s = (f.read() or "").strip()
            if not s:
                return None
            return int(s)
        except Exception:
            return None

    def _is_pid_alive(self, pid):
        if not pid:
            return False
        try:
            os.kill(pid, 0)
            return True
        except Exception:
            return False

    def _smart_status(self):
        pid = self._read_pid()
        if pid and self._is_pid_alive(pid):
            return True, pid
        return False, pid

    def _start_smart(self, interval_minutes=30):
        running, pid = self._smart_status()
        if running:
            return False, "智能模式已在运行（PID=%s）" % pid

        workdir = core.app_dir()
        out_path = os.path.join(workdir, "smart.out")
        cmd = ["python3", "retention_server.py", "--mode", "smart", "--interval", str(int(interval_minutes))]

        try:
            with open(out_path, "a", encoding="utf-8") as out:
                p = subprocess.Popen(cmd, cwd=workdir, stdout=out, stderr=out, close_fds=True)
            with open(self._smart_pid_file, "w", encoding="utf-8") as f:
                f.write(str(p.pid))
            return True, "✅ 已开启智能模式（每 %d 分钟运行一次每日更新），PID=%s\n日志：%s" % (
                int(interval_minutes), p.pid, out_path
            )
        except Exception as e:
            return False, "开启失败：%s" % e

    def _stop_smart(self):
        running, pid = self._smart_status()
        if not pid:
            return False, "未找到智能模式 PID（smart.pid 不存在或为空）"
        if not running:
            try:
                os.remove(self._smart_pid_file)
            except Exception:
                pass
            return False, "智能模式未在运行（PID=%s 已不存在），已清理 PID 记录" % pid

        try:
            os.kill(pid, signal.SIGTERM)
        except Exception as e:
            return False, "停止失败（无法发送 SIGTERM）：%s" % e

        t0 = time.time()
        while time.time() - t0 < 3.0:
            if not self._is_pid_alive(pid):
                try:
                    os.remove(self._smart_pid_file)
                except Exception:
                    pass
                return True, "✅ 已关闭智能模式（PID=%s）" % pid
            time.sleep(0.2)

        try:
            os.kill(pid, signal.SIGKILL)
        except Exception:
            pass
        try:
            os.remove(self._smart_pid_file)
        except Exception:
            pass
        return True, "✅ 已强制关闭智能模式（PID=%s）" % pid

    def _run_history_job_async(self, chat_id_in, start_date, end_date):
        def _tail_log(n=120):
            p = os.path.join(core.app_dir(), "retention_server.log")
            try:
                with open(p, "r", encoding="utf-8", errors="ignore") as f:
                    lines = f.readlines()
                tail = "".join(lines[-n:])
                return tail.strip()
            except Exception:
                return ""

        def _job():
            with self._history_lock:
                if self._history_running:
                    self._send_message(chat_id_in, "❌ 当前已有补历史任务在运行中，请等待完成后再发起。")
                    return
                self._history_running = True

            try:
                workdir = core.app_dir()
                cmd = ["python3", "retention_server.py", "--mode", "history", "--start", start_date, "--end", end_date]
                self._send_message(chat_id_in, "⏳ 开始补历史：%s ~ %s\n命令：%s" % (start_date, end_date, " ".join(cmd)))

                p = subprocess.Popen(
                    cmd,
                    cwd=workdir,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    universal_newlines=True,
                )
                out_lines = []
                try:
                    for line in p.stdout:
                        if line:
                            out_lines.append(line)
                            if len(out_lines) > 300:
                                out_lines = out_lines[-200:]
                except Exception:
                    pass
                rc = p.wait()

                tail = _tail_log(120)
                if rc == 0:
                    msg = "✅ 补历史完成：%s ~ %s" % (start_date, end_date)
                else:
                    msg = "❌ 补历史失败（退出码=%s）：%s ~ %s" % (rc, start_date, end_date)

                if tail:
                    msg += "\n\n【日志尾部】\n" + tail[-3500:]
                self._send_message(chat_id_in, msg)

            except Exception as e:
                self._send_message(chat_id_in, "❌ 补历史异常：%s" % e)
            finally:
                with self._history_lock:
                    self._history_running = False

        threading.Thread(target=_job, name="HistoryJob", daemon=True).start()

    def _send_message(self, chat_id, text):
        base_url = "https://api.telegram.org/bot%s" % self.global_cfg.bot_token

        def esc(s):
            s = str(s or "")
            return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

        try:
            raw = str(text or "")

            if "【金额区间统计】" in raw:
                lines = raw.splitlines()
                out_lines = []
                for ln in lines:
                    s = ln.strip()
                    if s == "【金额区间统计】":
                        out_lines.append("<b>%s</b>" % esc(s))
                        continue
                    m = re.match(r"^\s*(10元|11~30元|31~50元|51~99元|100元以上)：\s*$", ln)
                    if m:
                        name = m.group(1)
                        out_lines.append("<b>%s：</b>" % esc(name))
                        continue
                    out_lines.append(esc(ln))

                safe_html = "\n".join(out_lines)
                resp = requests.post(
                    base_url + "/sendMessage",
                    data={
                        "chat_id": str(chat_id),
                        "text": safe_html,
                        "parse_mode": "HTML",
                        "disable_web_page_preview": "true",
                    },
                    timeout=25,
                )
            else:
                resp = requests.post(
                    base_url + "/sendMessage",
                    data={
                        "chat_id": str(chat_id),
                        "text": raw,
                        "disable_web_page_preview": "true",
                    },
                    timeout=25,
                )

            if resp.status_code != 200:
                core.log("[TG] sendMessage HTTP %d: %s" % (resp.status_code, (resp.text or "")[:200]))
        except Exception as e:
            core.log("[TG] 发送消息失败：%s" % e)

    def _try_init_bot_info(self, base_url):
        try:
            resp = requests.get(base_url + "/getMe", timeout=20)
            if resp.status_code != 200:
                return
            js = resp.json()
            if not js.get("ok"):
                return
            result = js.get("result") or {}
            self._bot_id = result.get("id")
            self._bot_username = (result.get("username") or "").strip()
            if self._bot_id:
                core.log("[TG] bot信息：id=%s, username=@%s" % (self._bot_id, self._bot_username))
        except Exception:
            return

    def _cmd_name(self, text):
        t = (text or "").strip()
        if not t.startswith("/"):
            return ""
        cmd = t.split()[0][1:]
        cmd = cmd.split("@", 1)[0].strip()
        return cmd

    def _reply_group_id(self, chat, chat_id_in):
        title = (chat.get("title") or chat.get("username") or "").strip()
        msg = ("群：%s\n🆔 群组ID：%d" % (title, chat_id_in)) if title else ("🆔 群组ID：%d" % chat_id_in)
        self._send_message(chat_id_in, msg)

    def _parse_enabled_keywords(self, cfg, chat_id_in=None):
        return set(core.get_enabled_keywords_for_chat(chat_id_in, cfg))

    def _format_enabled_keywords(self, cfg, chat_id_in=None):
        items = core.get_enabled_keywords_for_chat(chat_id_in, cfg)
        return " / ".join(items) if items else "金额区间"

    def _parse_ratio_set_cmd(self, parts):
        if len(parts) != 3:
            return None, "用法：/设置比例 100 50"
        if (not re.fullmatch(r"\d+(?:\.\d+)?", parts[1])) or (not re.fullmatch(r"\d+(?:\.\d+)?", parts[2])):
            return None, "参数错误：阈值和比例必须是数字。示例：/设置比例 100 50"
        threshold = float(parts[1])
        percent = float(parts[2])
        if threshold < 0:
            return None, "阈值不能小于 0"
        if percent < 0 or percent > 100:
            return None, "比例必须在 0~100 之间"
        return {"threshold": threshold, "percent": percent}, ""

    def _run_loop(self):
        base_url = "https://api.telegram.org/bot%s" % self.global_cfg.bot_token
        offset = None
        core.log("[TG] 机器人线程启动（白名单/权限）")
        self._try_init_bot_info(base_url)

        while self._running:
            try:
                params = {"timeout": 20}
                if offset is not None:
                    params["offset"] = offset

                resp = requests.get(base_url + "/getUpdates", params=params, timeout=30)
                if resp.status_code != 200:
                    core.log("[TG] getUpdates HTTP %d: %s" % (resp.status_code, (resp.text or "")[:200]))
                    time.sleep(3)
                    continue

                data = resp.json()
                if not data.get("ok"):
                    time.sleep(3)
                    continue

                for update in data.get("result", []):
                    offset = update["update_id"] + 1
                    message = update.get("message") or update.get("edited_message")
                    if not message:
                        continue

                    chat = message.get("chat") or {}
                    chat_id_in = chat.get("id")
                    if chat_id_in is None:
                        continue
                    chat_id_in = int(chat_id_in)

                    text = (message.get("text") or "").strip()
                    if not text or not text.startswith("/"):
                        continue

                    from_user = message.get("from") or {}
                    chat_type = (chat.get("type") or "").strip().lower()
                    is_private = (chat_type == "private")

                    if self._cmd_name(text) == "群组id":
                        self._reply_group_id(chat, chat_id_in)
                        continue

                    if (not is_private) and (not self._is_allowed_chat(chat_id_in)):
                        if self._cmd_name(text) != "授权本群":
                            continue

                    reply = self.handle_command(text, chat_id_in, from_user, is_private=is_private)
                    if reply:
                        self._send_message(chat_id_in, reply)

            except Exception as e:
                core.log("[TG] 轮询异常：%s" % e)
                time.sleep(3)

        core.log("[TG] Telegram 机器人线程结束")

    def handle_command(self, full_text, chat_id_in, from_user, is_private=False):
        cmdline = full_text[1:].strip()
        if cmdline and "@" in cmdline.split(" ")[0]:
            first = cmdline.split(" ")[0].split("@", 1)[0]
            cmdline = first + " " + " ".join(cmdline.split(" ")[1:])

        parts = [p for p in cmdline.split() if p.strip()]
        if not parts:
            return ""

        uid = from_user.get("id")
        cmd0 = parts[0].strip()
        cmd0_l = cmd0.lower()

        if cmd0_l in ("help", "start", "帮助"):
            cfg_help = core.load_global_config_from_ini()
            enabled_text = self._format_enabled_keywords(cfg_help, None if is_private else chat_id_in)
            if is_private:
                group_help = (
                    "用法：/渠道 日期 功能词\n"
                    "示例：/t2002 2026-02-24 金额区间\n\n"
                    "工具：/群组id（获取本群 chat_id） /个人id（获取个人ID）\n"
                    "管理：/授权本群（加白本群） /白名单 /取消本群\n"
                    "管理员：/管理员（列表）\n"
                    "超管操作：/加管理员 123 /删管理员 123\n"
                    "群组维护：/加群 -100xxx /删群 -100xxx\n"
                    "参数示例：/加群 -1001234567890\n"
                    "注意：/加管理员 里填的是个人 user_id\n"
                )
            else:
                group_help = (
                                 "用法（当前本群支持：%s）：\n"
                                 "/t2002 2026-02-24 金额区间\n\n"
                                 "工具：/群组id（获取本群 chat_id） /个人id（获取个人ID）\n"
                                 "管理：/授权本群（加白本群） /白名单 /取消本群\n"
                                 "管理员：/管理员（列表）\n"
                                 "超管操作：/加管理员 123 /删管理员 123\n"
                                 "群组维护：/加群 -100xxx /删群 -100xxx\n"
                                 "参数示例：/加群 -1001234567890\n"
                                 "注意：/加管理员 里填的是个人 user_id\n"
                             ) % enabled_text
            if is_private and self._is_super_admin(uid):
                super_help = (
                    "\n【超管私聊功能】\n"
                    "1) 更新登录（自动更新 access_token + cookie）\n"
                    "   直接发送：/6位验证码（例如 /596695）\n\n"
                    "2) 智能模式（30分钟自动更新）\n"
                    "   /开启智能模式\n"
                    "   /关闭智能模式\n"
                    "   /查看状态\n"
                    "   /立即运行一次\n\n"
                    "3) 补历史（后台执行）\n"
                    "   /补历史 2026-02-21\n"
                    "   /补历史 2026-02-20 2026-02-28\n\n"
                    "4) 金额区间比例设置（仅私聊超管）\n"
                    "   /设置比例 100 50\n"
                    "   /关闭比例\n"
                    "   /查看比例\n"
                    "   /设置渠道比例 d30060 100 80\n"
                    "   /关闭渠道比例 d30060\n"
                )
                return group_help + super_help
            return group_help

        if is_private and self._is_super_admin(uid) and re.fullmatch(r"\d{6}", cmd0):
            ga = cmd0
            token, cookie_str, raw = retention_auth.login_with_gacode(ga)
            if not token:
                txt = str(raw)
                if len(txt) > 800:
                    txt = txt[:800] + "..."
                return "登录失败：%s" % txt

            p = retention_auth.save_login_result_to_ini(token, cookie_str)
            self.global_cfg.access_token = token
            self.global_cfg.token = cookie_str
            return "✅ 登录成功，access_token + cookie 已更新并写入配置：%s" % p

        if cmd0 == "个人id":
            return "🆔 你的个人ID：%s" % uid

        if cmd0 == "群组id":
            return "🆔 群组ID：%s" % chat_id_in

        if cmd0 == "授权本群":
            if not self._is_admin(uid):
                return "⛔ 无权限：仅管理员/超级管理员可加白。\n请发送 /个人id 给超级管理员添加。"
            ok, msg = self._whitelist_add_chat(chat_id_in)
            return msg if ok else ("❌ 加白失败：%s" % msg)

        if cmd0 == "取消本群":
            if not self._is_admin(uid):
                return "⛔ 无权限：仅管理员/超级管理员可操作。"
            ok, msg = self._whitelist_remove_chat(chat_id_in)
            return msg if ok else ("❌ 取消失败：%s" % msg)

        if cmd0 == "白名单":
            if not self._is_admin(uid):
                return "⛔ 无权限：仅管理员/超级管理员可查询白名单。"
            with self._lock:
                ids = sorted(self._allowed_chat_ids, key=lambda x: int(x))
            if not ids:
                return "白名单为空（尚未授权任何群）。"
            show = ids[:80]
            tail = "" if len(ids) <= 80 else ("\n…还有 %d 个未展示" % (len(ids) - 80))
            return "当前白名单群组ID：\n" + "\n".join(show) + tail

        if cmd0 in ("管理员", "管理员列表"):
            if not self._is_admin(uid):
                return "⛔ 无权限：仅管理员/超级管理员可查看。"
            return self._format_admin_list()

        if cmd0 in ("加管理员", "删管理员"):
            if not self._is_super_admin(uid):
                return "⛔ 无权限：仅超级管理员可操作。"
            if len(parts) < 2 or (not re.fullmatch(r"\d+", parts[1])):
                return "参数错误，示例：/%s 123456789" % cmd0
            target_uid = parts[1]
            if cmd0 == "加管理员":
                ok, msg = self._add_admin(target_uid)
                return msg
            ok, msg = self._remove_admin(target_uid)
            return msg

        if cmd0 in ("加群", "删群"):
            if not self._is_super_admin(uid):
                return "⛔ 无权限：仅超级管理员可操作。"
            if len(parts) < 2 or (not re.fullmatch(r"-?\d+", parts[1])):
                return "参数错误，示例：/%s -1001234567890" % cmd0
            target_chat = parts[1]
            if cmd0 == "加群":
                ok, msg = self._whitelist_add_chat(target_chat)
                return msg
            ok, msg = self._whitelist_remove_chat(target_chat)
            return msg

        if is_private and self._is_super_admin(uid) and cmd0 in (
        "开启智能模式", "关闭智能模式", "查看状态", "立即运行一次"):
            if cmd0 == "开启智能模式":
                ok, msg = self._start_smart(interval_minutes=30)
                return msg
            if cmd0 == "关闭智能模式":
                ok, msg = self._stop_smart()
                return msg
            if cmd0 == "查看状态":
                running, pid = self._smart_status()
                if running:
                    return "智能模式：运行中（PID=%s），每30分钟执行一次每日更新\n日志：%s" % (
                        pid, os.path.join(core.app_dir(), "smart.out")
                    )
                return "智能模式：未运行（PID=%s）" % (pid if pid else "无")

            def _job():
                try:
                    core.ensure_alias_map_file()
                    cfg = core.load_global_config_from_ini()
                    alias_map = core.load_alias_map()
                    msg = core.每日更新_全站点(cfg, alias_map)
                    self._send_message(chat_id_in, "✅ 立即运行完成：\n%s" % msg)
                except Exception as e:
                    self._send_message(chat_id_in, "❌ 立即运行失败：%s" % e)

            threading.Thread(target=_job, name="RunOnceDaily", daemon=True).start()
            return "已触发立即运行（后台执行中），完成后会再返回结果。"

        if cmd0 == "补历史":
            if not (is_private and self._is_super_admin(uid)):
                return ""
            if len(parts) == 2 and _is_date(parts[1]):
                start_date = parts[1]
                end_date = parts[1]
                self._run_history_job_async(chat_id_in, start_date, end_date)
                return ""
            if len(parts) == 3 and _is_date(parts[1]) and _is_date(parts[2]):
                start_date = parts[1]
                end_date = parts[2]
                self._run_history_job_async(chat_id_in, start_date, end_date)
                return ""
            return "用法：/补历史 2026-02-21  或  /补历史 2026-02-20 2026-02-28"

        if cmd0 == "设置比例":
            if not (is_private and self._is_super_admin(uid)):
                return ""
            parsed, err = self._parse_ratio_set_cmd(parts)
            if err:
                return err
            threshold = parsed["threshold"]
            percent = parsed["percent"]
            p = core.save_ini_defaults(
                {
                    "ratio_enabled": "1",
                    "ratio_threshold": (str(int(threshold)) if float(threshold).is_integer() else str(threshold)),
                    "ratio_percent": (str(int(percent)) if float(percent).is_integer() else str(percent)),
                }
            )
            self.global_cfg = core.load_global_config_from_ini()
            return (
                "✅ 比例规则已更新\n"
                "生效范围：首存金额 >= %s\n"
                "稳定保留：%s%%\n"
                "配置文件：%s"
            ) % (
                str(int(threshold)) if float(threshold).is_integer() else str(threshold),
                str(int(percent)) if float(percent).is_integer() else str(percent),
                p,
            )

        if cmd0 == "关闭比例":
            if not (is_private and self._is_super_admin(uid)):
                return ""
            p = core.save_ini_defaults({"ratio_enabled": "0"})
            self.global_cfg = core.load_global_config_from_ini()
            return "✅ 已关闭比例筛选，恢复原始金额区间展示\n配置文件：%s" % p

        if cmd0 == "查看比例":
            if not (is_private and self._is_super_admin(uid)):
                return ""

            cfg_now = core.load_global_config_from_ini()

            global_enabled = "开启" if str(getattr(cfg_now, "ratio_enabled", "0")) in (
                "1", "true", "yes", "on", "开启", "启用") else "关闭"
            global_threshold = getattr(cfg_now, "ratio_threshold", "100")
            global_percent = getattr(cfg_now, "ratio_percent", "90")

            result = []
            result.append("【全局默认配置】")
            result.append(f"状态：{global_enabled}")
            result.append(f"阈值：>= {global_threshold}")
            result.append(f"保留比例：{global_percent}%\n")
            result.append("【渠道单独配置】")

            channel_configs = []
            for key in dir(cfg_now):
                if key.startswith("channel_ratio_"):
                    channel = key.replace("channel_ratio_", "")
                    value = getattr(cfg_now, key, "")
                    if value:
                        parts = str(value).split(',')
                        if len(parts) == 3:
                            enabled = "开启" if parts[0] in ("1", "true", "yes", "on", "开启", "启用") else "关闭"
                            threshold = parts[1]
                            percent = parts[2]
                            channel_configs.append(f"{channel}：{enabled}，阈值≥{threshold}，保留{percent}%")

            if channel_configs:
                result.extend(channel_configs)
            else:
                result.append("暂无渠道单独配置")

            return "\n".join(result)

        if cmd0 == "设置渠道比例":
            if not (is_private and self._is_super_admin(uid)):
                return ""
            
            if len(parts) != 4:
                return "用法：/设置渠道比例 渠道名 阈值 比例\n示例：/设置渠道比例 d30060 100 80"
            
            channel = parts[1]
            if (not re.fullmatch(r"\d+(?:\.\d+)?", parts[2])) or (not re.fullmatch(r"\d+(?:\.\d+)?", parts[3])):
                return "参数错误：阈值和比例必须是数字"
            
            threshold = float(parts[2])
            percent = float(parts[3])
            
            if threshold < 0:
                return "阈值不能小于 0"
            if percent < 0 or percent > 100:
                return "比例必须在 0~100 之间"
            
            channel_key = channel.strip().lower()
            config_key = f"channel_ratio_{channel_key}"
            
            # 如果是整数，就保存为整数，不带.0
            threshold_str = str(int(threshold)) if threshold.is_integer() else str(threshold)
            percent_str = str(int(percent)) if percent.is_integer() else str(percent)
            config_value = f"1,{threshold_str},{percent_str}"
            
            updates = {config_key: config_value}
            p = core.save_ini_defaults(updates)
            self.global_cfg = core.load_global_config_from_ini()
            
            return f"✅ 渠道 [{channel}] 比例规则已设置\n阈值：>= {threshold_str}\n稳定保留：{percent_str}%\n配置文件：{p}"

        if cmd0 == "关闭渠道比例":
            if not (is_private and self._is_super_admin(uid)):
                return ""

            if len(parts) < 2:
                return "用法：/关闭渠道比例 渠道名\n示例：/关闭渠道比例 d30060"

            channel = parts[1]
            channel_key = channel.strip().lower()
            config_key = f"channel_ratio_{channel_key}"

            updates = {config_key: "0,0,0"}
            p = core.save_ini_defaults(updates)
            self.global_cfg = core.load_global_config_from_ini()

            return f"✅ 已关闭渠道 [{channel}] 的比例筛选\n配置文件：{p}"

        try:
            core.ensure_alias_map_file()
            alias_map = core.load_alias_map()
            cfg = core.load_global_config_from_ini()
            sites_all = core.build_sites(cfg, alias_map)
            if not sites_all:
                return "映射文件为空/无有效站点：%s" % core.ALIAS_MAP_FILE

            enabled_words = self._parse_enabled_keywords(cfg, None if is_private else chat_id_in)
            if not enabled_words:
                enabled_words = set(["金额区间"])

            if len(parts) != 3:
                return "当前仅支持格式：/渠道 日期 功能词"

            channel = parts[0]
            d_str = parts[1]
            func = parts[2]

            if not core.is_date_str(d_str):
                return "当前仅支持格式：/渠道 日期 功能词"

            if func not in enabled_words:
                return "当前本群仅支持：%s" % self._format_enabled_keywords(cfg, None if is_private else chat_id_in)

            if func == "金额区间":
                stats = core.calc_amount_ranges(sites_all, d_str, channel_filter=channel)
                return core.format_reply_amount_ranges(core.title_scope_channel(channel), d_str, stats)

            if func == "转换比":
                total_visits, total_reg, total_first = core.calc_visit_register_first(sites_all, d_str,
                                                                                      channel_filter=channel)
                return core.format_reply_convert(core.title_scope_channel(channel), d_str, total_visits, total_reg,
                                                 total_first)

            if func == "汇总":
                summary = core.compute_summary(sites_all, target_date=d_str, channel_filter=channel)
                return core.format_reply_summary(core.title_scope_channel(channel), d_str, summary)

            return "当前本群仅支持：%s" % self._format_enabled_keywords(cfg, None if is_private else chat_id_in)

        except Exception as e:
            return "执行指令出错：%s" % e
