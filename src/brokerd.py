#!/usr/bin/env python3
# SPDX-License-Identifier: MIT OR Apache-2.0
"""gpu-broker daemon: single-GPU priority queue with run-as-user and a
soft-enforcement watchdog. Runs as root under supervisord.

No cgroups / systemd / munge required — pure processes, pgids, signals and
`nvidia-smi --query-compute-apps`.
"""
import os
import sys
import json
import time
import errno
import socket
import struct
import signal
import threading
import subprocess
import pwd
import grp
import collections
import traceback

FINISHED_KEEP = 500          # how many finished jobs to remember (for `get`)

CONFIG_PATH = os.environ.get("GPU_BROKER_CONFIG", "/etc/gpu-broker/config.json")

DEFAULTS = {
    "gpu_index": 0,
    "socket_path": "/run/gpu-broker/sock",
    "state_dir": "/var/lib/gpu-broker",
    "log_dir": "/var/lib/gpu-broker/jobs",
    "daemon_log": "/var/lib/gpu-broker/brokerd.log",
    "queues": ["high", "normal", "ci"],          # priority order, first = highest
    "default_time_sec": 1200,                    # 20 min
    "max_time_sec": 43200,                       # 12h ceiling for --time override
    "kill_grace_sec": 10,                        # SIGTERM -> wait -> SIGKILL
    "watchdog_interval_sec": 3,
    "watchdog_action": "report",                 # "report" (log only) | "kill"
    "watchdog_whitelist_uids": [],               # uids whose stray compute is never touched
    "gpu_group": "gpu",                          # group granted to queued jobs (uvm gate)
    "gate_devices": ["/dev/nvidia-uvm", "/dev/nvidia-uvm-tools"],
    "gate_mode": "0660",                         # perms enforced on gate devices
    "docs": "https://github.com/ioxid/gpu-broker",   # where to read the instructions
    "docs_local": "/usr/local/share/doc/gpu-broker/README.md",
    "users": {                                   # uid/name -> allowed queues + default
        "github-runner": {"queues": ["ci"], "default": "ci"},
    },
    "default_user_policy": {"queues": ["high", "normal"], "default": "normal"},
}


def load_config():
    cfg = dict(DEFAULTS)
    try:
        with open(CONFIG_PATH) as f:
            cfg.update(json.load(f))
    except FileNotFoundError:
        pass
    return cfg


class Broker:
    def __init__(self, cfg):
        self.cfg = cfg
        self.lock = threading.RLock()
        self.pending = []          # list of job dicts (state=pending)
        self.running = None        # job dict or None
        self.counter = 0
        self.cancel_running = threading.Event()
        self._reported = set()     # watchdog: pids already logged (report mode)
        self.finished = collections.OrderedDict()   # id -> record (recent completed jobs)
        os.makedirs(cfg["state_dir"], exist_ok=True)
        os.makedirs(cfg["log_dir"], exist_ok=True)
        self.state_file = os.path.join(cfg["state_dir"], "state.json")
        self._load_state()

    # ---------- logging ----------
    def log(self, msg):
        line = "%s %s\n" % (time.strftime("%Y-%m-%d %H:%M:%S"), msg)
        try:
            with open(self.cfg["daemon_log"], "a") as f:
                f.write(line)
        except Exception:
            pass
        sys.stderr.write(line)
        sys.stderr.flush()

    # ---------- state persistence ----------
    def _persist(self):
        data = {"counter": self.counter, "pending": self.pending, "running": self.running,
                "finished": list(self.finished.values())[-FINISHED_KEEP:]}
        tmp = self.state_file + ".tmp"
        with open(tmp, "w") as f:
            json.dump(data, f)
        os.replace(tmp, self.state_file)

    def _load_state(self):
        try:
            with open(self.state_file) as f:
                data = json.load(f)
        except (FileNotFoundError, ValueError):
            return
        self.counter = data.get("counter", 0)
        self.pending = data.get("pending", [])
        for rec in data.get("finished", []):
            self.finished[rec["id"]] = rec
        run = data.get("running")
        if run and run.get("pgid") and pid_alive(run["pgid"]):
            # re-adopt a job that survived a daemon restart
            self.running = run
            self.log("recovered running job #%s (pgid %s)" % (run["id"], run["pgid"]))
        elif run:
            run["state"] = "failed"
            run["error"] = "daemon restarted while running"
            self.pending = self.pending  # leave pending intact
            self.running = None

    # ---------- queue policy ----------
    def user_policy(self, uid):
        name = uid_name(uid)
        users = self.cfg["users"]
        if name in users:
            return users[name]
        if str(uid) in users:
            return users[str(uid)]
        return self.cfg["default_user_policy"]

    def queue_rank(self, q):
        try:
            return self.cfg["queues"].index(q)
        except ValueError:
            return len(self.cfg["queues"])

    # ---------- submit ----------
    def submit(self, uid, gid, req):
        pol = self.user_policy(uid)
        queue = req.get("queue") or pol.get("default", self.cfg["queues"][-1])
        if queue not in self.cfg["queues"]:
            return {"ok": False, "error": "unknown queue %r" % queue}
        if queue not in pol["queues"]:
            return {"ok": False, "error": "user %s not allowed in queue %r (allowed: %s)"
                    % (uid_name(uid), queue, ",".join(pol["queues"]))}
        cmd = req.get("cmd")
        if not cmd or not isinstance(cmd, list):
            return {"ok": False, "error": "empty command"}
        t = req.get("time")
        time_sec = self.cfg["default_time_sec"] if not t else int(t)
        capped = min(time_sec, self.cfg["max_time_sec"])
        with self.lock:
            self.counter += 1
            jid = self.counter
            job = {
                "id": jid,
                "uid": uid, "gid": gid, "user": uid_name(uid),
                "queue": queue,
                "cmd": cmd,
                "cwd": req.get("cwd") or pwd.getpwuid(uid).pw_dir,
                "env": req.get("env") or {},
                "name": req.get("name") or "",
                "time_sec": capped,
                "state": "pending",
                "submit_ts": time.time(),
                "log": os.path.join(self.cfg["log_dir"], "%d.log" % jid),
                "pgid": None, "start_ts": None, "end_ts": None, "exit_code": None,
            }
            self.pending.append(job)
            self._persist()
        warn = ""
        if capped < time_sec:
            warn = " (time capped to %ds ceiling)" % capped
        return {"ok": True, "id": jid, "queue": queue, "time_sec": capped,
                "log": job["log"], "warn": warn}

    def list_jobs(self, history=0):
        with self.lock:
            run = dict(self.running) if self.running else None
            if run:
                run["elapsed"] = time.time() - (run.get("start_ts") or time.time())
            pend = sorted(self.pending, key=lambda j: (self.queue_rank(j["queue"]), j["submit_ts"]))
            resp = {"ok": True, "running": run, "pending": [scrub(j) for j in pend]}
            if history:
                recent = list(self.finished.values())[-history:]
                resp["finished"] = list(reversed(recent))   # newest first
            return resp

    def cancel(self, uid, jid):
        with self.lock:
            if self.running and self.running["id"] == jid:
                if uid not in (0, self.running["uid"]):
                    return {"ok": False, "error": "not your job"}
                self.cancel_running.set()
                return {"ok": True, "msg": "cancelling running job #%d" % jid}
            for j in self.pending:
                if j["id"] == jid:
                    if uid not in (0, j["uid"]):
                        return {"ok": False, "error": "not your job"}
                    self.pending.remove(j)
                    self._persist()
                    return {"ok": True, "msg": "removed pending job #%d" % jid}
        return {"ok": False, "error": "no such job #%d" % jid}

    def get_job(self, jid):
        with self.lock:
            if self.running and self.running["id"] == jid:
                rec = job_record(self.running)
                rec["elapsed"] = time.time() - (self.running.get("start_ts") or time.time())
                return {"ok": True, "job": rec}
            for j in self.pending:
                if j["id"] == jid:
                    return {"ok": True, "job": job_record(j)}
            if jid in self.finished:
                return {"ok": True, "job": self.finished[jid]}
        return {"ok": False, "error": "no such job #%d" % jid}

    def find_log(self, jid):
        with self.lock:
            for j in ([self.running] if self.running else []) + self.pending:
                if j and j["id"] == jid:
                    return {"ok": True, "path": j["log"]}
        p = os.path.join(self.cfg["log_dir"], "%d.log" % jid)
        if os.path.exists(p):
            return {"ok": True, "path": p}
        return {"ok": False, "error": "no such job #%d" % jid}

    # ---------- scheduler ----------
    def pick_next(self):
        with self.lock:
            if not self.pending:
                return None
            self.pending.sort(key=lambda j: (self.queue_rank(j["queue"]), j["submit_ts"]))
            return self.pending.pop(0)

    def scheduler_loop(self):
        # if we recovered a running job, monitor it first
        if self.running:
            self._monitor(self.running)
        while True:
            job = self.pick_next()
            if job is None:
                time.sleep(0.5)
                continue
            self._launch(job)
            self._monitor(job)

    def _launch(self, job):
        with self.lock:
            self.cancel_running.clear()
            job["state"] = "running"
            job["start_ts"] = time.time()
            self.running = job
        # pre-create log owned by the submitting user
        try:
            fd = os.open(job["log"], os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o644)
            os.close(fd)
            os.chown(job["log"], job["uid"], job["gid"])
        except Exception as e:
            self.log("log setup failed for #%s: %s" % (job["id"], e))
        pid = os.fork()
        if pid == 0:
            self._child_exec(job)        # never returns
            os._exit(127)
        with self.lock:
            job["pgid"] = pid            # setsid in child => pgid == pid
            self._persist()
        self.log("started #%s user=%s queue=%s pgid=%s limit=%ss cmd=%s"
                 % (job["id"], job["user"], job["queue"], pid, job["time_sec"], job["cmd"]))

    def _child_exec(self, job):
        try:
            os.setsid()
            pw = pwd.getpwuid(job["uid"])
            if os.geteuid() == 0:
                # the job's normal groups + the gpu group, so it (and only it) can
                # open uvm. Skipped when running rootless (e.g. the test harness).
                groups = os.getgrouplist(pw.pw_name, job["gid"])
                ggid = self.gpu_gid()
                if ggid is not None and ggid not in groups:
                    groups = groups + [ggid]
                os.setgroups(groups)
                os.setgid(job["gid"])
                os.setuid(job["uid"])
            cwd = job["cwd"]
            try:
                # Fall back to the user's home if the requested cwd is missing OR
                # unreadable by this user (e.g. submitted from another user's
                # private dir) — otherwise chdir would fail here, before the log
                # fds are wired up, and the job would die with an opaque exit 127.
                os.chdir(cwd)
            except OSError:
                os.chdir(pw.pw_dir)
            env = dict(job.get("env") or {})
            env["USER"] = env["LOGNAME"] = pw.pw_name
            env["HOME"] = pw.pw_dir
            env.setdefault("PATH", "/usr/local/bin:/usr/bin:/bin")
            env["CUDA_VISIBLE_DEVICES"] = str(self.cfg["gpu_index"])
            env["NVIDIA_VISIBLE_DEVICES"] = str(self.cfg["gpu_index"])
            env["GPU_BROKER_JOB"] = str(job["id"])
            devnull = os.open(os.devnull, os.O_RDONLY)
            os.dup2(devnull, 0)
            logfd = os.open(job["log"], os.O_WRONLY | os.O_APPEND)
            os.dup2(logfd, 1)
            os.dup2(logfd, 2)
            os.execvpe(job["cmd"][0], job["cmd"], env)
        except Exception:
            try:
                os.write(2, ("gpu-broker: exec failed: %s\n" % traceback.format_exc()).encode())
            except Exception:
                pass
            os._exit(127)

    def _monitor(self, job):
        pgid = job["pgid"]
        deadline = job["start_ts"] + job["time_sec"]
        termed_at = None
        while True:
            try:
                wpid, status = os.waitpid(pgid, os.WNOHANG)
                # wpid==0 => our child exists and has NOT exited. That is
                # authoritative: do NOT also probe the process *group* here, as
                # it only comes into being once the child reaches setsid() and a
                # pre-setsid probe would race us into a false "done".
                if wpid != 0:
                    self._finish(job, status)
                    return
            except ChildProcessError:
                # recovered job (not our child, re-adopted across a daemon
                # restart): we cannot waitpid it, so fall back to a group probe.
                if not pid_alive(pgid):
                    self._finish(job, 0)
                    return
            now = time.time()
            reason = None
            if self.cancel_running.is_set():
                reason = "cancelled"
            elif now > deadline:
                reason = "timeout"
            if reason and termed_at is None:
                self.log("#%s %s -> SIGTERM pgid %s" % (job["id"], reason, pgid))
                killpg(pgid, signal.SIGTERM)
                termed_at = now
                job["state"] = reason
            if termed_at and now - termed_at > self.cfg["kill_grace_sec"]:
                self.log("#%s grace expired -> SIGKILL pgid %s" % (job["id"], pgid))
                killpg(pgid, signal.SIGKILL)
            # Poll finely: short jobs are reaped (and the slot freed for the next
            # queued job / lease) promptly, not up to a second late.
            time.sleep(0.2)

    def _finish(self, job, status):
        with self.lock:
            job["end_ts"] = time.time()
            if os.WIFEXITED(status):
                job["exit_code"] = os.WEXITSTATUS(status)
            elif os.WIFSIGNALED(status):
                job["exit_code"] = -os.WTERMSIG(status)
            if job["state"] not in ("timeout", "cancelled"):
                job["state"] = "done" if job["exit_code"] == 0 else "failed"
            killpg(job["pgid"], signal.SIGKILL)   # reap any stragglers in the group
            self.running = None
            self.finished[job["id"]] = job_record(job)
            while len(self.finished) > FINISHED_KEEP:
                self.finished.popitem(last=False)
            self._persist()
        self.log("finished #%s state=%s exit=%s" % (job["id"], job["state"], job["exit_code"]))

    # ---------- uvm device gate (primary soft enforcement) ----------
    def gpu_gid(self):
        try:
            return grp.getgrnam(self.cfg.get("gpu_group", "gpu")).gr_gid
        except KeyError:
            return None

    def ensure_gate(self):
        """Keep gate devices owned root:<gpu_group> with restricted perms, so only
        processes carrying the gpu group (i.e. jobs the broker launched) can open
        them. CUDA needs /dev/nvidia-uvm -> out-of-queue compute fails with EACCES."""
        if not self.cfg.get("gate_enabled", True):
            return
        gid = self.gpu_gid()
        if gid is None:
            return
        mode = self.cfg.get("gate_mode", "0660")
        if isinstance(mode, str):
            mode = int(mode, 8)
        for dev in self.cfg.get("gate_devices", []):
            try:
                st = os.stat(dev)
            except FileNotFoundError:
                continue
            if st.st_gid != gid or (st.st_mode & 0o777) != mode:
                try:
                    os.chown(dev, 0, gid)
                    os.chmod(dev, mode)
                    self.log("gate: %s -> root:%s %04o" % (dev, self.cfg["gpu_group"], mode))
                except PermissionError as e:
                    self.log("gate: cannot set perms on %s: %s" % (dev, e))

    # ---------- watchdog (audit / backstop) ----------
    def watchdog_loop(self):
        interval = self.cfg["watchdog_interval_sec"]
        whitelist = set(self.cfg.get("watchdog_whitelist_uids", []))
        while True:
            try:
                self.ensure_gate()
                self._watchdog_tick(whitelist)
            except Exception as e:
                self.log("watchdog error: %s" % e)
            time.sleep(interval)

    def _watchdog_tick(self, whitelist):
        compute = compute_pids()
        if not compute:
            self._reported.clear()
            return
        with self.lock:
            job = self.running
            allowed = job_pids(job["pgid"]) if job else set()
        action = self.cfg.get("watchdog_action", "report")
        mypid = os.getpid()
        seen = set()
        for pid in compute:
            seen.add(pid)
            if pid in allowed or pid == mypid or pid == 1:
                continue
            try:
                puid = proc_uid(pid)
            except Exception:
                continue
            if puid in whitelist:
                continue
            if action == "kill":
                self.log("watchdog: killing out-of-queue GPU pid %s (uid %s)" % (pid, puid))
                try:
                    os.kill(pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                except PermissionError:
                    self.log("watchdog: no permission to kill %s" % pid)
            elif action == "notify":
                if pid not in self._reported:
                    self._reported.add(pid)
                    ttys = self._notify(pid, puid)
                    self.log("watchdog: nudged %s about out-of-queue GPU pid %s (ttys: %s)"
                             % (uid_name(puid), pid, ",".join(sorted(ttys)) or "none"))
            elif self.cfg.get("gate_enabled", True) and pid not in self._reported:
                self._reported.add(pid)
                self.log("watchdog: out-of-queue GPU compute pid %s (user %s) -- it bypassed "
                         "the uvm gate (NVENC/Vulkan?). Not killed (report mode)."
                         % (pid, uid_name(puid)))
        self._reported &= seen

    def _offender_ttys(self, pid, uid):
        """Terminals to message: the offending process's own controlling pts (via its
        std fds) plus all interactive login ttys of that user (from `who`)."""
        ttys = set()
        for fd in ("0", "1", "2"):
            try:
                tgt = os.readlink("/proc/%d/fd/%s" % (pid, fd))
            except OSError:
                continue
            if tgt.startswith("/dev/pts/") or tgt.startswith("/dev/tty"):
                ttys.add(tgt)
        try:
            name = uid_name(uid)
            for line in subprocess.check_output(["who"], timeout=5).decode().splitlines():
                p = line.split()
                if len(p) >= 2 and p[0] == name:
                    dev = "/dev/" + p[1]
                    if os.path.exists(dev):
                        ttys.add(dev)
        except Exception:
            pass
        return ttys

    def _notify(self, pid, uid):
        docs = self.cfg.get("docs", "https://github.com/ioxid/gpu-broker")
        local = self.cfg.get("docs_local", "/usr/local/share/doc/gpu-broker/README.md")
        msg = ("\r\n\033[1;33m[gpu-broker]\033[0m %s: процесс %d использует GPU в обход очереди — "
               "так делать не нужно.\r\n"
               "Запускай GPU-задачи через \033[1mgpu-submit\033[0m (очередь: gpu-q).\r\n"
               "Инструкция: %s\r\n         (локально: %s)\r\n"
               "Процесс не остановлен — это напоминание.\r\n"
               % (uid_name(uid), pid, docs, local))
        ttys = self._offender_ttys(pid, uid)
        for t in ttys:
            try:
                fd = os.open(t, os.O_WRONLY | os.O_NONBLOCK)
                try:
                    os.write(fd, msg.encode())
                finally:
                    os.close(fd)
            except OSError:
                pass
        return ttys

    # ---------- socket server ----------
    def serve(self):
        sp = self.cfg["socket_path"]
        os.makedirs(os.path.dirname(sp), exist_ok=True)
        try:
            os.unlink(sp)
        except FileNotFoundError:
            pass
        srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        srv.bind(sp)
        os.chmod(sp, 0o666)            # any local user may submit; identity from SO_PEERCRED
        srv.listen(64)
        self.log("listening on %s" % sp)
        while True:
            conn, _ = srv.accept()
            threading.Thread(target=self._handle, args=(conn,), daemon=True).start()

    def _handle(self, conn):
        try:
            creds = conn.getsockopt(socket.SOL_SOCKET, SO_PEERCRED, struct.calcsize("3i"))
            _, uid, gid = struct.unpack("3i", creds)
            data = b""
            while not data.endswith(b"\n"):
                chunk = conn.recv(65536)
                if not chunk:
                    break
                data += chunk
            req = json.loads(data.decode() or "{}")
            op = req.get("op")
            if op == "submit":
                resp = self.submit(uid, gid, req)
            elif op in ("list", "status"):
                resp = self.list_jobs(int(req.get("history", 0)))
            elif op == "cancel":
                resp = self.cancel(uid, int(req["id"]))
            elif op == "log":
                resp = self.find_log(int(req["id"]))
            elif op == "get":
                resp = self.get_job(int(req["id"]))
            elif op == "ping":
                resp = {"ok": True, "pong": True}
            else:
                resp = {"ok": False, "error": "unknown op %r" % op}
            conn.sendall((json.dumps(resp) + "\n").encode())
        except Exception:
            try:
                conn.sendall((json.dumps({"ok": False, "error": traceback.format_exc()}) + "\n").encode())
            except Exception:
                pass
        finally:
            conn.close()


# ---------- helpers ----------
SO_PEERCRED = 17  # Linux


def scrub(j):
    return {k: j.get(k) for k in ("id", "user", "queue", "name", "state",
                                  "time_sec", "submit_ts", "start_ts", "log")}


def job_record(j):
    return {k: j.get(k) for k in ("id", "user", "queue", "name", "state", "exit_code",
                                  "time_sec", "submit_ts", "start_ts", "end_ts", "log")}


def uid_name(uid):
    try:
        return pwd.getpwuid(uid).pw_name
    except KeyError:
        return str(uid)


def pid_alive(pid):
    try:
        os.killpg(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False


def killpg(pgid, sig):
    try:
        os.killpg(pgid, sig)
    except (ProcessLookupError, PermissionError, OSError):
        pass


def compute_pids():
    """PIDs of processes holding a GPU compute context."""
    fake = os.environ.get("GPU_BROKER_FAKE_COMPUTE")
    if fake:
        try:
            with open(fake) as f:
                return set(int(x) for x in f.read().split())
        except Exception:
            return set()
    # Run as an unprivileged user: NVML on init tries to chmod /dev/nvidia-uvm
    # back to 0666; as non-root that chmod fails (EPERM) so our gate survives.
    # (As root it would succeed and silently re-open the gate every tick.)
    try:
        out = subprocess.check_output(
            ["nvidia-smi", "--query-compute-apps=pid", "--format=csv,noheader"],
            stderr=subprocess.DEVNULL, timeout=10,
            user="nobody", group="nogroup").decode()
    except Exception:
        return set()
    pids = set()
    for line in out.splitlines():
        line = line.strip()
        if line.isdigit():
            pids.add(int(line))
    return pids


def proc_uid(pid):
    return os.stat("/proc/%d" % pid).st_uid


def job_pids(pgid):
    """All pids belonging to the running job: same process-group OR descendants
    of the leader (robust to children that call setsid)."""
    if not pgid:
        return set()
    allowed = set()
    ppid_of = {}
    pgid_of = {}
    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue
        pid = int(entry)
        try:
            with open("/proc/%d/stat" % pid) as f:
                fields = f.read().rsplit(")", 1)[1].split()
            # after "comm)": state ppid pgrp ...  -> fields[0]=state,1=ppid,2=pgrp
            ppid_of[pid] = int(fields[1])
            pgid_of[pid] = int(fields[2])
        except (FileNotFoundError, ProcessLookupError, IndexError, ValueError):
            continue
    for pid, pg in pgid_of.items():
        if pg == pgid:
            allowed.add(pid)
    # descendant closure of the leader
    changed = True
    allowed.add(pgid)
    while changed:
        changed = False
        for pid, ppid in ppid_of.items():
            if ppid in allowed and pid not in allowed:
                allowed.add(pid)
                changed = True
    return allowed


def main():
    cfg = load_config()
    b = Broker(cfg)
    b.log("gpu-broker starting (gpu %s, queues %s)" % (cfg["gpu_index"], cfg["queues"]))
    if b.gpu_gid() is None:
        b.log("WARNING: group %r missing -- uvm gate disabled until it exists"
              % cfg.get("gpu_group", "gpu"))
    b.ensure_gate()
    threading.Thread(target=b.watchdog_loop, daemon=True).start()
    threading.Thread(target=b.scheduler_loop, daemon=True).start()
    b.serve()


if __name__ == "__main__":
    main()
