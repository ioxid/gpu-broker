# bash completion for gpu-broker CLIs

_gpu_queues() {
    python3 -c "import json;print(' '.join(json.load(open('/etc/gpu-broker/config.json'))['queues']))" 2>/dev/null
}
_gpu_ids() {
    command gpu-q 2>/dev/null | grep -oE '#[0-9]+' | tr -d '#'
}

_gpu_submit() {
    local cur prev
    cur="${COMP_WORDS[COMP_CWORD]}"; prev="${COMP_WORDS[COMP_CWORD-1]}"
    case "$prev" in
        -q|--queue) COMPREPLY=( $(compgen -W "$(_gpu_queues)" -- "$cur") ); return;;
        -t|--time|-n|--name) return;;
    esac
    if [[ "$cur" == -* ]]; then
        COMPREPLY=( $(compgen -W "-q --queue -t --time -n --name -w --follow -h --help --" -- "$cur") )
    fi
}
_gpu_id_cmd() {
    local cur="${COMP_WORDS[COMP_CWORD]}"
    COMPREPLY=( $(compgen -W "$(_gpu_ids) -f --follow" -- "$cur") )
}
complete -F _gpu_submit gpu-submit
complete -F _gpu_id_cmd gpu-log
complete -F _gpu_id_cmd gpu-cancel
complete -o default gpu-q
