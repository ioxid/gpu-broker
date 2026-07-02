#!/usr/bin/env python3
# SPDX-License-Identifier: MIT OR Apache-2.0
"""gpu-broker client. Installed as gpu-submit / gpu-q / gpu-log / gpu-cancel
(dispatch by argv[0] basename)."""
import os
import sys
import json
import time
import socket
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


DISPATCH = {
    "gpu-submit": cmd_submit,
    "gpu-q": cmd_q,
    "gpu-log": cmd_log,
    "gpu-cancel": cmd_cancel,
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
    sys.exit("usage: gpu-submit | gpu-q | gpu-log | gpu-cancel")


if __name__ == "__main__":
    main()
