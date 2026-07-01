#!/usr/bin/env python3
# SPDX-License-Identifier: MIT OR Apache-2.0
"""Isolated self-test for gpu-broker.

Spins up a PRIVATE broker instance (own socket / state dir / queues, gate
disabled, fake GPU) and asserts its behaviour. Does NOT touch the production
daemon, the real GPU, or other users. Runs rootless.

    gpu-broker-test            # run all
    gpu-broker-test -v         # show daemon log on failure
"""
import os, sys, json, time, socket, struct, subprocess, tempfile, signal, shutil, pwd

LIB = os.path.dirname(os.path.realpath(__file__))  # realpath: resolve the bin symlink
BROKERD = os.path.join(LIB, "brokerd.py")
sys.path.insert(0, LIB)
import brokerd  # for in-process unit tests

VERBOSE = "-v" in sys.argv
ME = os.getuid()
MYNAME = pwd.getpwuid(ME).pw_name
PASS, FAIL = 0, 0


def check(name, cond, extra=""):
    global PASS, FAIL
    if cond:
        PASS += 1; print("  \033[32mPASS\033[0m", name)
    else:
        FAIL += 1; print("  \033[31mFAIL\033[0m", name, extra)


# ───────────────────────── A. in-process unit tests ─────────────────────────
def unit_tests():
    print("[unit] pure logic (no daemon, no root)")
    cfg = dict(brokerd.DEFAULTS)
    cfg["users"] = {"github-runner": {"queues": ["ci"], "default": "ci"}}
    cfg["state_dir"] = tempfile.mkdtemp()
    cfg["log_dir"] = cfg["state_dir"]
    cfg["daemon_log"] = os.path.join(cfg["state_dir"], "d.log")
    b = brokerd.Broker(cfg)

    check("queue priority rank high<normal<ci",
          b.queue_rank("high") < b.queue_rank("normal") < b.queue_rank("ci"))

    # github-runner forced to ci, cannot use high
    try:
        gr = pwd.getpwnam("github-runner").pw_uid
    except KeyError:
        gr = 1002
    r = b.submit(gr, gr, {"cmd": ["true"], "queue": "high"})
    check("restricted user rejected from disallowed queue", not r["ok"], r)
    r = b.submit(gr, gr, {"cmd": ["true"]})              # default
    check("restricted user default queue = ci", r["ok"] and r["queue"] == "ci", r)

    r = b.submit(ME, ME, {"cmd": ["true"], "queue": "bogus"})
    check("unknown queue rejected", not r["ok"], r)

    r = b.submit(ME, ME, {"cmd": ["true"], "time": 999999})
    check("time capped to ceiling", r["time_sec"] <= cfg["max_time_sec"], r)

    # job_pids: our own pgid is included
    mypg = os.getpgid(0)
    check("job_pids includes own pgid members", os.getpid() in brokerd.job_pids(mypg))
    shutil.rmtree(cfg["state_dir"], ignore_errors=True)


# ───────────────────────── integration harness ─────────────────────────
class Inst:
    def __init__(self, **overrides):
        self.dir = tempfile.mkdtemp(prefix="gpubroker-test-")
        self.fake = os.path.join(self.dir, "fake_compute")
        open(self.fake, "w").close()
        cfg = {
            "socket_path": os.path.join(self.dir, "sock"),
            "state_dir": self.dir,
            "log_dir": os.path.join(self.dir, "jobs"),
            "daemon_log": os.path.join(self.dir, "brokerd.log"),
            "queues": ["high", "normal", "ci"],
            "default_time_sec": 1200, "max_time_sec": 3600,
            "kill_grace_sec": 2, "watchdog_interval_sec": 1,
            "watchdog_action": "report", "gate_enabled": False,
            "users": {}, "default_user_policy":
                {"queues": ["high", "normal", "ci"], "default": "normal"},
        }
        cfg.update(overrides)
        self.cfg_path = os.path.join(self.dir, "config.json")
        json.dump(cfg, open(self.cfg_path, "w"))
        self.cfg = cfg
        self.proc = None

    def start(self):
        env = dict(os.environ, GPU_BROKER_CONFIG=self.cfg_path,
                   GPU_BROKER_FAKE_COMPUTE=self.fake)
        self.errf = open(os.path.join(self.dir, "daemon.stderr"), "w+")
        self.proc = subprocess.Popen(["/usr/bin/python3", BROKERD], env=env,
                                     stdout=self.errf, stderr=self.errf)
        for _ in range(50):
            if os.path.exists(self.cfg["socket_path"]):
                return
            if self.proc.poll() is not None:
                break
            time.sleep(0.1)
        self.errf.seek(0)
        raise RuntimeError("daemon did not come up:\n" + self.errf.read())

    def stop(self, sig=signal.SIGTERM):
        if self.proc:
            self.proc.send_signal(sig)
            try: self.proc.wait(5)
            except subprocess.TimeoutExpired: self.proc.kill()

    def call(self, req):
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.connect(self.cfg["socket_path"])
        s.sendall((json.dumps(req) + "\n").encode())
        data = b""
        while not data.endswith(b"\n"):
            c = s.recv(65536)
            if not c: break
            data += c
        s.close()
        return json.loads(data)

    def submit(self, cmd, queue=None, name="", time_sec=None):
        req = {"op": "submit", "cmd": ["bash", "-c", cmd], "queue": queue,
               "name": name, "cwd": self.dir, "env": {}}
        if time_sec: req["time"] = time_sec
        return self.call(req)

    def jlog(self, jid):
        try: return open(os.path.join(self.cfg["log_dir"], "%d.log" % jid)).read()
        except FileNotFoundError: return ""

    def dump_log(self):
        if VERBOSE:
            print("--- daemon log ---")
            print(open(self.cfg["daemon_log"]).read())


def test_run_and_user():
    print("[int] submit / run-as-user / logs")
    I = Inst(); I.start()
    try:
        r = I.submit("id -un; echo done")
        check("submit ok", r["ok"], r)
        for _ in range(50):
            if "done" in I.jlog(r["id"]): break
            time.sleep(0.1)
        log = I.jlog(r["id"])
        check("job executed", "done" in log, log)
        check("ran as submitting user", MYNAME in log, log)
    finally:
        I.dump_log(); I.stop(); shutil.rmtree(I.dir, ignore_errors=True)


def test_priority_order():
    print("[int] priority ordering high>normal>ci + FIFO")
    I = Inst(); I.start()
    try:
        I.submit("sleep 4", queue="normal", name="blocker")   # occupy slot
        time.sleep(0.5)
        a = I.submit("echo x", queue="ci", name="c")
        b = I.submit("echo x", queue="high", name="h")
        c = I.submit("echo x", queue="normal", name="n")
        time.sleep(0.3)
        lst = I.call({"op": "list"})
        order = [j["queue"] for j in lst["pending"]]
        check("pending sorted high,normal,ci", order == ["high", "normal", "ci"], order)
        # wait for completion, check start order via finish log
        time.sleep(6)
        fin = open(I.cfg["daemon_log"]).read()
        # extract started order of the 3 quick jobs
        started = [l for l in fin.splitlines() if "started #" in l]
        ids = [int(l.split("started #")[1].split()[0]) for l in started]
        order_exec = ids[1:4]  # after blocker
        check("executed high(%d) before normal(%d) before ci(%d)"
              % (b["id"], c["id"], a["id"]),
              order_exec == [b["id"], c["id"], a["id"]], order_exec)
    finally:
        I.dump_log(); I.stop(); shutil.rmtree(I.dir, ignore_errors=True)


def test_timelimit():
    print("[int] timelimit kills overrunning job")
    I = Inst(kill_grace_sec=1); I.start()
    try:
        r = I.submit("echo begin; sleep 30; echo SHOULD_NOT", time_sec=2)
        time.sleep(6)
        log = I.jlog(r["id"])
        check("job started", "begin" in log, log)
        check("job killed before finishing", "SHOULD_NOT" not in log, log)
        dl = open(I.cfg["daemon_log"]).read()
        check("logged as timeout", "timeout" in dl)
    finally:
        I.dump_log(); I.stop(); shutil.rmtree(I.dir, ignore_errors=True)


def test_cancel():
    print("[int] cancel pending and running")
    I = Inst(); I.start()
    try:
        run = I.submit("sleep 20", queue="normal", name="run")
        pend = I.submit("sleep 20", queue="normal", name="pend")
        time.sleep(1)
        rc = I.call({"op": "cancel", "id": pend["id"]})
        check("cancel pending ok", rc["ok"], rc)
        rc = I.call({"op": "cancel", "id": run["id"]})
        check("cancel running ok", rc["ok"], rc)
        time.sleep(2)
        lst = I.call({"op": "list"})
        check("queue empty after cancels",
              lst["running"] is None and not lst["pending"], lst)
    finally:
        I.dump_log(); I.stop(); shutil.rmtree(I.dir, ignore_errors=True)


def test_recovery():
    print("[int] crash recovery: running job re-adopted across daemon restart")
    I = Inst(); I.start()
    try:
        r = I.submit("sleep 8", queue="normal", name="survivor")
        time.sleep(1.5)
        pgid = json.load(open(os.path.join(I.dir, "state.json")))["running"]["pgid"]
        check("job has pgid and is alive", brokerd.pid_alive(pgid), pgid)
        I.stop(signal.SIGKILL)              # hard-kill daemon, job keeps running
        time.sleep(1)
        I.start()                            # restart same config
        time.sleep(1)
        lst = I.call({"op": "list"})
        check("job re-adopted as running",
              lst["running"] and lst["running"]["id"] == r["id"], lst.get("running"))
        check("job process survived restart", brokerd.pid_alive(pgid))
    finally:
        I.dump_log(); I.stop(); shutil.rmtree(I.dir, ignore_errors=True)


def test_exitcode():
    print("[int] finished job exit code retrievable via get")
    I = Inst(); I.start()
    try:
        r = I.submit("exit 7")
        j = None
        for _ in range(50):
            j = I.call({"op": "get", "id": r["id"]})["job"]
            if j["state"] in ("done", "failed", "timeout", "cancelled"):
                break
            time.sleep(0.1)
        check("exit_code recorded == 7", j.get("exit_code") == 7, j)
        check("nonzero exit -> state failed", j["state"] == "failed", j)
        ok = I.submit("true")
        for _ in range(50):
            jo = I.call({"op": "get", "id": ok["id"]})["job"]
            if jo["state"] in ("done", "failed"):
                break
            time.sleep(0.1)
        check("zero exit -> state done", jo["state"] == "done" and jo["exit_code"] == 0, jo)
    finally:
        I.dump_log(); I.stop(); shutil.rmtree(I.dir, ignore_errors=True)


def test_notify():
    print("[int] watchdog (notify mode): message reaches offender's terminal, no kill")
    I = Inst(watchdog_action="notify", watchdog_interval_sec=1); I.start()
    mfd, sfd = os.openpty()
    try:
        # a stray 'compute' process whose stdout IS a terminal (the pty slave)
        s = subprocess.Popen(["sleep", "60"], stdout=sfd)
        os.close(sfd)
        open(I.fake, "w").write(str(s.pid))
        time.sleep(2.5)
        data = b""
        try:
            os.set_blocking(mfd, False)
            data = os.read(mfd, 8192)
        except (BlockingIOError, OSError):
            pass
        check("message delivered to offender's terminal", b"gpu-broker" in data, data[:80])
        check("offender NOT killed (notify != kill)", s.poll() is None)
    finally:
        try: s.kill()
        except Exception: pass
        os.close(mfd)
        I.dump_log(); I.stop(); shutil.rmtree(I.dir, ignore_errors=True)


def test_watchdog():
    print("[int] watchdog (kill mode): out-of-slot compute killed, in-slot spared")
    I = Inst(watchdog_action="kill", watchdog_interval_sec=1); I.start()
    try:
        # A: idle slot, a stray 'compute' process -> killed
        s = subprocess.Popen(["sleep", "60"])
        open(I.fake, "w").write(str(s.pid))
        time.sleep(2.5)
        check("out-of-slot compute killed", s.poll() is not None and s.returncode == -9,
              "rc=%s" % s.returncode)
        # B: a running job's own pid is 'compute' -> spared
        r = I.submit("sleep 8", queue="normal")
        time.sleep(1.5)
        pgid = json.load(open(os.path.join(I.dir, "state.json")))["running"]["pgid"]
        open(I.fake, "w").write(str(pgid))
        time.sleep(2.5)
        check("in-slot compute spared", brokerd.pid_alive(pgid))
    finally:
        try: s.kill()
        except Exception: pass
        I.dump_log(); I.stop(); shutil.rmtree(I.dir, ignore_errors=True)


def main():
    print("== gpu-broker self-test ==  (isolated instance, rootless, no real GPU)\n")
    unit_tests()
    for t in (test_run_and_user, test_priority_order, test_timelimit,
              test_cancel, test_recovery, test_exitcode, test_notify, test_watchdog):
        try:
            t()
        except Exception as e:
            global FAIL; FAIL += 1
            print("  \033[31mFAIL\033[0m %s raised %r" % (t.__name__, e))
    print("\n== %d passed, %d failed ==" % (PASS, FAIL))
    sys.exit(1 if FAIL else 0)


if __name__ == "__main__":
    main()
