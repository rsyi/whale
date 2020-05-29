#!/bin/bash
cd ~/.metaframe/metadata
dataframe_file="$(pwd)/$(~/.metaframe/bin/fzf --preview 'cat {}')"
if [ -f "${dataframe_file}" ]; then
    vim "${dataframe_file}"
fi
#    popd >&-
#    trap popd >&-
