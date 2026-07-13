#!/usr/bin/env python3
# SPDX-License-Identifier: MIT OR Apache-2.0
"""gpu-broker client. Installed as gpu-submit / gpu-q / gpu-log / gpu-cancel /
gpu-lease (dispatch by argv[0] basename)."""
import os
import sys
import json
import time
import socket
import select
import argparse

CONFIG_PATH = os.environ.get("GPU_BROKER_CONFIG", "/etc/gpu-broker/config.json")


def socket_path():
    try:
        with open(CONFIG_PATH) as f:
            return json.load(f).get("socket_path", "/run/gpu-broker/sock")
    except Exception:
        return "/run/gpu-broker/sock"


def call(req):
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        s.connect(socket_path())
    except (FileNotFoundError, ConnectionRefusedError):
        sys.exit("gpu-broker: daemon not reachable (is gpu-brokerd running?)")
    s.sendall((json.dumps(req) + "\n").encode())
    data = b""
    while not data.endswith(b"\n"):
        chunk = s.recv(65536)
        if not chunk:
            break
        data += chunk
    s.close()
    return json.loads(data.decode())


def parse_time(s):
    s = s.strip().lower()
    mult = 1
    if s and s[-1] in "smh":
        mult = {"s": 1, "m": 60, "h": 3600}[s[-1]]
        s = s[:-1]
    return int(float(s) * mult)


def fmt_dur(sec):
    sec = int(sec)
    h, sec = divmod(sec, 3600)
    m, sec = divmod(sec, 60)
    if h:
        return "%dh%02dm" % (h, m)
    if m:
        return "%dm%02ds" % (m, sec)
    return "%ds" % sec


def cmd_submit(argv):
    p = argparse.ArgumentParser(prog="gpu-submit",
                                usage="gpu-submit [--queue Q] [--time T] [--description D] [-w|-F] -- CMD ...")
    p.add_argument("--queue", "-q", default=None)
    p.add_argument("--time", "-t", default=None, help="e.g. 30m, 2h, 90s (default from config)")
    p.add_argument("--description", "-d", default="",
                   help="say what this run is (shown in gpu-q / history) — the more, the better")
    p.add_argument("--follow", "-w", action="store_true", help="stream output until job ends")
    p.add_argument("--foreground", "-F", action="store_true",
                   help="run the command here on the foreground (your stdin/tty/uid/cwd/env) "
                        "instead of in the daemon, while holding the GPU via the lease "
                        "protocol; output is teed to your stdout AND the broker job log")
    if "--" in argv:
        i = argv.index("--")
        opts, cmd = argv[:i], argv[i + 1:]
    else:
        opts, cmd = argv, []
    a = p.parse_args(opts)
    if not cmd:
        p.error("no command given (put it after --)")
    if a.foreground:
        if a.follow:
            p.error("--foreground and --follow are mutually exclusive")
        sys.exit(run_foreground(a, cmd))
    req = {"op": "submit", "queue": a.queue, "description": a.description,
           "cmd": cmd, "cwd": os.getcwd(),
           "env": {k: v for k, v in os.environ.items()}}
    if a.time:
        req["time"] = parse_time(a.time)
    r = call(req)
    if not r.get("ok"):
        sys.exit("submit failed: " + r.get("error", "?"))
    print("submitted job #%d  queue=%s  limit=%s  log=%s%s"
          % (r["id"], r["queue"], fmt_dur(r["time_sec"]), r["log"], r.get("warn", "")))
    if a.follow:
        sys.exit(follow_job(r["id"], r["log"]))   # exit with the job's own status


def run_foreground(a, cmd):
    """Foreground counterpart to a plain submit: reserve the GPU slot via the
    lease protocol (a first-class slot record — shows this command's description
    and real exit code in history, no placeholder process), but run CMD right
    here as a child of this shell — real stdin/tty, this user, this cwd/env — and
    tee its output to BOTH our stdout and the broker job log so it still shows up
    in gpu-q / gpu-log. Release on exit and propagate the command's exit status.
    """
    import signal as _signal
    import threading
    import subprocess

    description = a.description or os.path.basename(cmd[0])

    def do_log(msg):
        sys.stderr.write("gpu-submit: " + msg + "\n")
        sys.stderr.flush()

    status, jid, logpath = lease_acquire(a.queue, description, a.time, do_log=do_log)
    if status != "granted":
        detail = status.split(":", 1)[-1] if ":" in status else status
        sys.exit("gpu-submit --foreground: did not get the GPU (%s)" % detail)
    do_log("GPU granted (#%d); running on foreground" % jid)

    # The daemon created the lease's log at grant time and chowned it to us, so
    # we can append the real command's output into it (gpu-log still works).
    try:
        logf = open(logpath, "a", buffering=1)
    except OSError:
        logf = None

    def finish(code):
        if logf:
            try:
                logf.write("\n[command exited: %s]\n" % code)
                logf.close()
            except Exception:
                pass
        lease_release(jid, code)   # records the real exit in history + frees slot

    try:
        proc = subprocess.Popen(cmd, stdin=None,             # inherit our stdin (foreground)
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    except OSError as e:
        finish(127)
        sys.exit("gpu-submit --foreground: cannot run %r: %s" % (cmd[0], e))

    # Forward terminal signals to the child so Ctrl-C etc. behave as usual.
    def _forward(signum, _frame):
        try:
            proc.send_signal(signum)
        except Exception:
            pass
    for s in (_signal.SIGINT, _signal.SIGTERM, _signal.SIGHUP, _signal.SIGQUIT):
        try:
            _signal.signal(s, _forward)
        except (OSError, ValueError):
            pass

    # If the broker revokes the slot under us (timeout/cancel/daemon restart),
    # stop the command — we no longer hold the GPU.
    revoked = {"hit": False}

    def _watch():
        while proc.poll() is None:
            j = get_job(jid)
            if not j or j["state"] != "running":
                revoked["hit"] = True
                do_log("GPU lease revoked (%s) -> stopping command"
                       % (j["state"] if j else "gone"))
                try:
                    proc.terminate()
                except Exception:
                    pass
                return
            time.sleep(1.0)
    threading.Thread(target=_watch, daemon=True).start()

    out = sys.stdout.buffer
    for chunk in iter(lambda: proc.stdout.read(4096), b""):
        out.write(chunk)
        out.flush()
        if logf:
            try:
                logf.write(chunk.decode("utf-8", "replace"))
            except Exception:
                pass
    code = proc.wait()
    if code < 0:
        code = 128 + (-code)                # killed by signal N -> 128+N
    finish(code)
    if revoked["hit"] and code == 0:
        code = 125                          # command ended but we lost the lease
    return code


def fmt_clock(ts):
    return time.strftime("%H:%M", time.localtime(ts)) if ts else "--:--"


def cmd_q(argv):
    p = argparse.ArgumentParser(
        prog="gpu-q",
        description="Show the GPU queue: what is running and what is waiting. "
                    "With -a, also show recently finished jobs (history).")
    p.add_argument("-a", "--all", action="store_true", help="also show recently finished jobs")
    p.add_argument("-n", "--limit", type=int, default=15, metavar="N",
                   help="how many finished jobs to show with -a (default 15)")
    a = p.parse_args(argv)
    req = {"op": "list"}
    if a.all:
        req["history"] = a.limit
    r = call(req)
    run = r.get("running")
    print("=== running ===")
    if run:
        print("  #%-4d %-12s %-7s %-11s %s"
              % (run["id"], run["user"], run["queue"],
                 fmt_dur(run.get("elapsed", 0)) + "/" + fmt_dur(run["time_sec"]),
                 run.get("description", "")))
    else:
        print("  (GPU idle)")
    pend = r.get("pending", [])
    print("=== pending (%d) ===" % len(pend))
    for j in pend:
        print("  #%-4d %-12s %-7s limit=%-6s %s"
              % (j["id"], j["user"], j["queue"], fmt_dur(j["time_sec"]), j.get("description", "")))
    if a.all:
        fin = r.get("finished", [])
        print("=== recent (%d) ===" % len(fin))
        for j in fin:
            st, st_e = j.get("start_ts"), j.get("end_ts")
            dur = fmt_dur(st_e - st) if st and st_e else "-"
            ec = j.get("exit_code")
            print("  #%-4d %-12s %-7s %-9s exit=%-4s %-7s ended %s  %s"
                  % (j["id"], j.get("user", ""), j.get("queue", ""), j.get("state", ""),
                     "-" if ec is None else ec, dur, fmt_clock(st_e), j.get("description", "")))


def cmd_log(argv):
    p = argparse.ArgumentParser(prog="gpu-log")
    p.add_argument("id", type=int)
    p.add_argument("-f", "--follow", action="store_true")
    a = p.parse_args(argv)
    r = call({"op": "log", "id": a.id})
    if not r.get("ok"):
        sys.exit(r.get("error", "?"))
    path = r["path"]
    if a.follow:
        follow_file(path, lambda: job_active(a.id))
    else:
        try:
            with open(path) as f:
                sys.stdout.write(f.read())
        except FileNotFoundError:
            sys.exit("log not found: " + path)


def cmd_cancel(argv):
    p = argparse.ArgumentParser(prog="gpu-cancel")
    p.add_argument("id", type=int)
    a = p.parse_args(argv)
    r = call({"op": "cancel", "id": a.id})
    print(r.get("msg") if r.get("ok") else "cancel failed: " + r.get("error", "?"))


FINISHED = {"done", "failed", "timeout", "cancelled"}


def get_job(jid):
    r = call({"op": "get", "id": jid})
    return r.get("job") if r.get("ok") else None


def job_active(jid):
    j = get_job(jid)
    if not j or j["state"] in FINISHED:
        return None
    return j["state"]


def exit_code_for(job):
    ec = job.get("exit_code")
    if ec is None:
        return 0
    return 128 + (-ec) if ec < 0 else ec     # killed by signal N -> 128+N (shell convention)


def lease_acquire(queue, description, time_str, do_log=lambda _msg: None, should_abort=None):
    """Reserve the GPU slot via the daemon `lease` op (a first-class slot record,
    no placeholder process) and wait until the broker grants it. Returns
    (status, jid, logpath):
      'granted'         - the slot is ours
      'aborted'         - should_abort() fired before grant (we released it)
      'revoked:<state>' - broker finished the lease before granting
      'error:<msg>'     - could not even queue it
    Progress lines go to do_log() (defaults to a no-op)."""
    req = {"op": "lease", "queue": queue, "description": description}
    if time_str:
        req["time"] = parse_time(time_str)
    r = call(req)
    if not r.get("ok"):
        return ("error:" + r.get("error", "?"), None, None)
    jid, logpath = r["id"], r["log"]
    do_log("queued #%d on %s (limit %s); waiting for GPU..."
           % (jid, r["queue"], fmt_dur(r["time_sec"])))
    last = None
    while True:
        if should_abort and should_abort():
            lease_release(jid)
            return ("aborted", jid, logpath)
        j = get_job(jid)
        if j is None:
            return ("revoked:gone", jid, logpath)
        st = j["state"]
        if st == "running":
            return ("granted", jid, logpath)
        if st in FINISHED:
            return ("revoked:" + st, jid, logpath)
        if st != last:
            do_log("job #%d %s..." % (jid, st))
            last = st
        time.sleep(0.3)


def lease_release(jid, exit_code=0):
    """Release a held (or still-pending) lease, recording exit_code in history."""
    return call({"op": "release", "id": jid, "exit_code": exit_code})


def follow_job(jid, path):
    state = None
    while True:
        j = get_job(jid)
        if j is None:
            print("... job not found ...")
            return 0
        st = j["state"]
        if st != state:
            if st == "pending":
                print("... queued, waiting for GPU ...")
            elif st == "running":
                print("... running ...")
            state = st
        if st == "running":
            follow_file(path, lambda: (get_job(jid) or {}).get("state") == "running")
            j = get_job(jid) or j
            return _report(j)
        if st in FINISHED:
            follow_file(path, lambda: False)     # flush whatever the log has
            return _report(j)
        time.sleep(1)


def _report(j):
    code = exit_code_for(j)
    sys.stderr.write("\n[job #%d %s, exit %s]\n" % (j["id"], j["state"], j.get("exit_code")))
    return code


def follow_file(path, still_active):
    # wait for the file, then tail it until the job is no longer active
    for _ in range(100):
        if os.path.exists(path):
            break
        time.sleep(0.2)
    try:
        f = open(path)
    except FileNotFoundError:
        return
    with f:
        while True:
            chunk = f.read()
            if chunk:
                sys.stdout.write(chunk)
                sys.stdout.flush()
            elif not still_active():
                rest = f.read()
                if rest:
                    sys.stdout.write(rest)
                break
            else:
                time.sleep(0.4)


def cmd_lease(argv):
    """Cooperative GPU lease helper (see PARASOL_ZK_GPU_LEASE_CMD in
    parasol-dex-zk-api). Line contract with the caller:

      * we reserve the broker's single GPU slot (a first-class lease record, no
        placeholder process); the real GPU work runs in the *caller's* process;
      * when the broker grants us the slot we print exactly ``GRANTED`` on stdout;
      * if the broker takes the slot back under us (timeout/cancel/crash) we
        print ``REVOKED <reason>`` on stdout;
      * the caller releases by closing our stdin (EOF): we release the slot so it
        frees immediately, then exit 0;
      * queue-wait progress is logged to stderr, never stdout.
    """
    p = argparse.ArgumentParser(
        prog="gpu-lease",
        description="Acquire a cooperative GPU slot from the broker and hold it "
                    "until stdin closes. Prints GRANTED / REVOKED <reason> on "
                    "stdout; wait progress on stderr.")
    p.add_argument("--queue", "-q", default=None)
    p.add_argument("--time", "-t", default=None,
                   help="max hold, e.g. 1h (broker-capped); frees the slot if we die")
    p.add_argument("--description", "-d", default="gpu-lease",
                   help="what is using the GPU (shown in gpu-q)")
    a = p.parse_args(argv)

    def say(tag):                         # the stdout line contract
        sys.stdout.write(tag + "\n")
        sys.stdout.flush()

    def do_log(msg):                        # progress -> stderr only
        sys.stderr.write("gpu-lease: " + msg + "\n")
        sys.stderr.flush()

    def stdin_eof():
        """True once the caller closes our stdin (its release signal). Reads and
        discards anything sent; a closed pipe reports readable + read()==b''."""
        try:
            fd = sys.stdin.fileno()
        except (ValueError, OSError):
            return True                   # no stdin at all -> treat as released
        while select.select([fd], [], [], 0)[0]:
            try:
                if os.read(fd, 4096) == b"":
                    return True
            except OSError:
                return True
        return False

    def wait_or_release(seconds):
        """Sleep up to `seconds`, but return early (True) the moment stdin
        closes. Single-threaded, so shutdown is always clean."""
        try:
            fd = sys.stdin.fileno()
        except (ValueError, OSError):
            return True
        if select.select([fd], [], [], seconds)[0]:
            return stdin_eof()
        return False

    status, jid, _ = lease_acquire(a.queue, a.description, a.time,
                                   do_log=do_log, should_abort=stdin_eof)
    if status == "aborted":
        do_log("released #%d before grant" % jid)
        sys.exit(0)
    if status.startswith("error:"):
        do_log("lease failed: " + status[len("error:"):])
        say("REVOKED lease-failed")
        sys.exit(1)
    if status.startswith("revoked:"):
        say("REVOKED " + status[len("revoked:"):])
        sys.exit(1)

    say("GRANTED")
    do_log("granted slot (#%d); holding until stdin closes" % jid)

    # Hold the slot until the caller releases (stdin EOF) or the broker revokes
    # it under us (timeout/cancel/crash).
    while True:
        if wait_or_release(1.0):
            lease_release(jid, 0)
            do_log("released #%d" % jid)
            sys.exit(0)
        j = get_job(jid)
        st = j["state"] if j else "gone"
        if st != "running":
            say("REVOKED " + st)
            sys.exit(1)


DISPATCH = {
    "gpu-submit": cmd_submit,
    "gpu-q": cmd_q,
    "gpu-log": cmd_log,
    "gpu-cancel": cmd_cancel,
    "gpu-lease": cmd_lease,
}


def main():
    base = os.path.basename(sys.argv[0])
    if base in DISPATCH:
        DISPATCH[base](sys.argv[1:])
        return
    # `gpu <sub> ...`
    if len(sys.argv) > 1 and ("gpu-" + sys.argv[1]) in DISPATCH:
        DISPATCH["gpu-" + sys.argv[1]](sys.argv[2:])
        return
    sys.exit("usage: gpu-submit | gpu-q | gpu-log | gpu-cancel | gpu-lease")


if __name__ == "__main__":
    main()
