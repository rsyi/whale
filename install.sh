prefix='~/.metaframe/metaframe'
update_config=1
shells="bash zsh"

append_line() {
    set -e

    local update line file pat lno
    update="$1"
    line="$2"
    file="$3"
    pat="${4:-}"
    lno=""

    echo "Update $file:"
    echo "  - $line"
    if [ -f "$file" ]; then
    if [ $# -lt 4 ]; then
        lno=$(\grep -nF "$line" "$file" | sed 's/:.*//' | tr '\n' ' ')
    else
        lno=$(\grep -nF "$pat" "$file" | sed 's/:.*//' | tr '\n' ' ')
    fi
    fi
    if [ -n "$lno" ]; then
    echo "    - Already exists: line #$lno"
    else
    if [ $update -eq 1 ]; then
        [ -f "$file" ] && echo >> "$file"
        echo "$line" >> "$file"
        echo "    + Added"
    else
        echo "    ~ Skipped"
    fi
    fi
    echo
    set +e
}

for shell in $shells; do
    [ $shell = zsh ] && dest=${ZDOTDIR:-~}/.zshrc || dest=~/.bashrc
    append_line $update_config "[ -f ${prefix}.sh ] && source ${prefix}.sh" "$dest" "${prefix}.sh"
done
