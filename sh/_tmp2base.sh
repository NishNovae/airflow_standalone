#!/bin/bash

CURR_DT=$1
DEL_DT=$2

user="root"
password="qwer123"
database="history_db"

mysql --local-infile=1 -u"$user" -p"$password"<<EOF
DELETE FROM history_db.cmd_usage WHERE dt = '${CURR_DT}';

INSERT INTO history_db.cmd_usage
SELECT CASE WHEN dt LIKE '%-%-%'
    THEN STR_TO_DATE(dt, '%Y-%m-%d')
    ELSE STR_TO_DATE('1970-01-01', '%Y-%m-%d')
    END AS dt,
    command,
    CASE WHEN cnt REGEXP '[0-9]+%'
    THEN CAST(cnt AS UNSIGNED)
    ELSE -1
    END AS cnt
    FROM history_db.tmp_cmd_usage
    WHERE dt = '${CURR_DT}';
EOF
