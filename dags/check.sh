#!/bin/bash
echo 'check'
DONE_PATH=~/data/done/{{ds_nodash}}
DONE_PATH_FILE="${DONE_PATH}/_DONE"

if [[ -e "$DONE_PATH_FILE" ]]; then
    figlet "Let's move on!"
else
    echo "I'll be back => $DONE_PATH_FILE"
fi
