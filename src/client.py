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
                                usage="gpu-submit [--queue Q] [--time T] [--name N] [-w] -- CMD ...")
    p.add_argument("--queue", "-q", default=None)
    p.add_argument("--time", "-t", default=None, help="e.g. 30m, 2h, 90s (default from config)")
    p.add_argument("--name", "-n", default="")
    p.add_argument("--follow", "-w", action="store_true", help="stream output until job ends")
    if "--" in argv:
        i = argv.index("--")
        opts, cmd = argv[:i], argv[i + 1:]
    else:
        opts, cmd = argv, []
    a = p.parse_args(opts)
    if not cmd:
        p.error("no command given (put it after --)")
    req = {"op": "submit", "queue": a.queue, "name": a.name,
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
                 run.get("name", "")))
    else:
        print("  (GPU idle)")
    pend = r.get("pending", [])
    print("=== pending (%d) ===" % len(pend))
    for j in pend:
        print("  #%-4d %-12s %-7s limit=%-6s %s"
              % (j["id"], j["user"], j["queue"], fmt_dur(j["time_sec"]), j.get("name", "")))
    if a.all:
        fin = r.get("finished", [])
        print("=== recent (%d) ===" % len(fin))
        for j in fin:
            st, st_e = j.get("start_ts"), j.get("end_ts")
            dur = fmt_dur(st_e - st) if st and st_e else "-"
            ec = j.get("exit_code")
            print("  #%-4d %-12s %-7s %-9s exit=%-4s %-7s ended %s  %s"
                  % (j["id"], j.get("user", ""), j.get("queue", ""), j.get("state", ""),
                     "-" if ec is None else ec, dur, fmt_clock(st_e), j.get("name", "")))


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

      * we submit a placeholder "holder" job that occupies the broker's single
        GPU slot; the real GPU work runs in the *caller's* process;
      * when the broker grants us the slot (holder starts running) we print
        exactly ``GRANTED`` on stdout;
      * if the broker takes the slot back under us (timeout/cancel/crash) we
        print ``REVOKED <reason>`` on stdout;
      * the caller releases by closing our stdin (EOF): we cancel the holder so
        the slot frees immediately, then exit 0;
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
    p.add_argument("--name", "-n", default="gpu-lease")
    a = p.parse_args(argv)

    def say(tag):                         # the stdout line contract
        sys.stdout.write(tag + "\n")
        sys.stdout.flush()

    def note(msg):                        # progress -> stderr only
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

    # The holder just parks the GPU slot; the real GPU work runs in the caller's
    # process. Use a numeric sleep (portable) sized to the lease window — we
    # cancel it on release, and the broker's own --time cap reaps it anyway if
    # the caller dies without releasing (so a crash can't hold the GPU forever).
    hold_sec = parse_time(a.time) if a.time else 86400
    req = {"op": "submit", "queue": a.queue, "name": a.name,
           "cmd": ["sleep", str(hold_sec)], "cwd": "/", "env": {}}
    if a.time:
        req["time"] = hold_sec
    r = call(req)
    if not r.get("ok"):
        note("submit failed: " + r.get("error", "?"))
        say("REVOKED submit-failed")
        sys.exit(1)
    jid = r["id"]
    note("queued job #%d on %s (limit %s); waiting for GPU..."
         % (jid, r["queue"], fmt_dur(r["time_sec"])))

    def release_and_exit(code, before_grant=False):
        # Cancel the holder so the next queued job starts at once.
        call({"op": "cancel", "id": jid})
        note("released #%d%s" % (jid, " before grant" if before_grant else ""))
        sys.exit(code)

    # Phase 1: wait for the broker to grant the slot (holder -> running). Bail if
    # the caller gives up (stdin EOF) while we are still queued.
    last = None
    while True:
        if stdin_eof():
            release_and_exit(0, before_grant=True)
        j = get_job(jid)
        if j is None:
            say("REVOKED job-vanished")
            sys.exit(1)
        st = j["state"]
        if st == "running":
            break
        if st in FINISHED:                # cancelled/timed out/failed before grant
            say("REVOKED " + st)
            sys.exit(1)
        if st != last:
            note("job #%d %s..." % (jid, st))
            last = st
        time.sleep(0.3)

    say("GRANTED")
    note("granted slot (job #%d); holding until stdin closes" % jid)

    # Phase 2: hold the slot until the caller releases (stdin EOF) or the broker
    # revokes it under us (timeout/cancel/crash).
    while True:
        if wait_or_release(1.0):
            release_and_exit(0)
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
