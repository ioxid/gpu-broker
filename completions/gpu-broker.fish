# fish completion for gpu-broker CLIs

function __gpu_queues
    python3 -c "import json;print('\n'.join(json.load(open('/etc/gpu-broker/config.json'))['queues']))" 2>/dev/null
end

function __gpu_ids
    # running + pending job ids, with a short description
    command gpu-q 2>/dev/null | string replace -rf '^\s*#(\d+)\s+(\S+)\s+(\S+).*' '$1\t$2 $3'
end

# ---- gpu-submit ----
complete -c gpu-submit -f
complete -c gpu-submit -s q -l queue  -x -a '(__gpu_queues)' -d 'priority queue'
complete -c gpu-submit -s t -l time   -x -d 'time limit, e.g. 30m 2h 90s'
complete -c gpu-submit -s n -l name   -x -d 'job name'
complete -c gpu-submit -s w -l follow -d 'stream output until job ends'
complete -c gpu-submit -s h -l help   -d 'show help'

# ---- gpu-q (no args) ----
complete -c gpu-q -f

# ---- gpu-log ----
complete -c gpu-log -f -a '(__gpu_ids)' -d 'job'
complete -c gpu-log -s f -l follow -d 'follow log output'

# ---- gpu-cancel ----
complete -c gpu-cancel -f -a '(__gpu_ids)' -d 'job'
