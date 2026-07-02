# gpu-broker

A tiny **single-GPU job queue** for shared boxes: run GPU jobs one at a time, in
priority order, as the submitting user, with time limits and logs. No Slurm, no
cgroups, no systemd, no root cluster software — just a small Python daemon,
process groups, signals and `nvidia-smi`.

Built for the common case: **one GPU, several users** (people + CI) fighting over
the card. Instead of `python train.py`, you write `gpu-submit -- python train.py`.

## Features

- **Priority queues** (`high > normal > ci`, configurable), FIFO within a queue,
  non-preemptive. Which queues a user may use is configurable (e.g. CI is pinned
  to a low-priority `ci` queue).
- **Run-as-submitter**: jobs run as the user who submitted them (uid taken from
  `SO_PEERCRED`, un-spoofable), in their cwd, with their environment.
- **Time limits** with graceful `SIGTERM` → `SIGKILL`.
- **Live logs**, follow mode, exit-code propagation (`gpu-submit -w` exits with the
  job's own code — CI-friendly).
- **Crash recovery**: a running job is re-adopted across a daemon restart.
- **Soft enforcement** (optional): GPU compute *outside* the queue can be made to
  fail — via a `/dev/nvidia-uvm` permission gate (CUDA needs uvm; `nvidia-smi` does
  not, so monitoring keeps working for everyone), and/or a watchdog that can log,
  message the offender's terminal, or kill.

## Requirements

- Linux, NVIDIA driver + `nvidia-smi`, Python 3.8+.
- A process supervisor to keep the daemon alive (examples for `supervisord`
  included; anything works).
- Root for the daemon (so it can run jobs as arbitrary users and maintain the
  device gate). Jobs themselves run unprivileged as the submitter.

## Install

```sh
sudo ./install.sh          # installs to /usr/local + /etc/gpu-broker, creates the gpu group
sudo /usr/local/bin/supervisord -c /etc/gpu-broker/supervisord.conf   # start the daemon
```

This lays down:

| Path | What |
|---|---|
| `/usr/local/lib/gpu-broker/` | `brokerd.py`, `client.py`, `selftest.py` |
| `/usr/local/bin/gpu-{submit,q,log,cancel}`, `gpu-broker-test` | CLIs |
| `/etc/gpu-broker/config.json` | config |
| `/var/lib/gpu-broker/` | state + per-job logs |
| `/run/gpu-broker/sock` | control socket |

## Usage

```sh
gpu-submit -- python train.py --epochs 100     # queue a job (returns #id, runs in background)
gpu-submit -w -- python train.py               # -w: wait, stream output, exit with job's code
gpu-submit -q high -t 4h -n bigrun -- ./run.sh # priority queue, 4h limit, a name
gpu-submit -- bash -c 'cd ~/p && ./run.sh'     # shell constructs: wrap in bash -c

gpu-q                 # what's running and what's queued
gpu-q -a              # + recently finished jobs (history)
gpu-log 42            # a job's output   (gpu-log 42 -f  to follow)
gpu-cancel 42         # drop from queue / stop if running
```

`gpu-submit` flags: `-q/--queue`, `-t/--time` (`30m`,`2h`,`90s`), `-n/--name`,
`-w/--follow`. The command goes after `--`.

### For scripts / CI

`gpu-submit -w` blocks until the job ends and exits with its code (killed-by-signal
N → 128+N):

```sh
gpu-submit -q ci -w -- pytest && echo pass || echo "failed ($?)"
```

The control socket speaks JSON (`{"op":"submit"|"list"|"get"|"log"|"cancel"|"ping"}`);
`get` returns a job record including `state` and `exit_code`.

## Queues, priority, time

- Next to run is always the highest-priority waiting job; FIFO within a queue.
  Non-preemptive (a running job finishes; priority only reorders the waiting set).
- Per-user queue policy prevents e.g. CI from jumping ahead of humans.
- Default time limit is configurable (`default_time_sec`), overridable with `-t`
  up to `max_time_sec`.

## Enforcement (optional, soft)

Nothing above stops someone from using the GPU *without* the queue. Two independent
mechanisms make out-of-queue use fail or get noticed:

- **uvm gate** (`gate_enabled: true`): the daemon keeps `/dev/nvidia-uvm` owned
  `root:<gpu_group>` mode `0660`. CUDA requires uvm, so compute outside the queue
  fails at init with a permission error. The daemon grants the gpu group only to
  jobs it launches, so only queued jobs can compute. `nvidia-smi`/NVML does not need
  uvm, so monitoring keeps working for everyone. (Root/sudo users can bypass.)
- **watchdog** (`watchdog_action`): `report` (log only), `notify` (write a message
  to the offender's terminal pointing them at this doc), or `kill` (SIGKILL).
  Catches things that slip past the gate (e.g. NVENC/Vulkan paths that don't use uvm).

## Run as a service

The daemon must be kept alive and started on boot. A `supervisord` program config is
in `supervisord.conf`; an example boot hook (`/etc/rc.local`-style) is in
`rc.local.example`. Adapt to your init of choice.

## Testing

```sh
gpu-broker-test         # isolated, rootless, no real GPU — spins a private instance
```

## Configuration

`config.json` keys: `queues` (order = priority), `default_time_sec`/`max_time_sec`,
`users` (user → allowed queues + default), `default_user_policy`, `gate_enabled`,
`gpu_group`, `gate_devices`, `watchdog_action`, `docs` (URL shown in notify messages).
See `config.example.json`.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option. Unless you explicitly state otherwise, any contribution
intentionally submitted for inclusion in this work by you, as defined in the
Apache-2.0 license, shall be dual licensed as above, without any additional terms
or conditions.
