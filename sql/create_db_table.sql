USE history_db;

CREATE TABLE IF NOT EXISTS history_db.tmp_cmd_usage (
	dt VARCHAR(500),
	command VARCHAR(500),
	cnt VARCHAR(500)
);

-- USE history_db;

CREATE TABLE IF NOT EXISTS history_db.cmd_usage (
	dt DATE,
	command VARCHAR(500),
	cnt INT,
    tmp_dt VARCHAR(500)
);
