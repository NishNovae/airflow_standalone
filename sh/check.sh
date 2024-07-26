#!/bin/bash
DATE=$1 # {{ ds_nodash }}
TASK=$2 # {{ prefix for _DONE }}

echo 'checking _DONE flag at...'

DONE_PATH=~/db/done/$DATE
DONE_PATH_FILE=$DONE_PATH/${TASK}_DONE

echo "$DONE_PATH_FILE"

if [[ -e "$DONE_PATH_FILE" ]]; then
    figlet "Let's move on!"
else
    echo "I'll be back => $DONE_PATH_FILE"
    exit 1
fi
