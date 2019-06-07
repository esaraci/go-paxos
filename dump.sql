BEGIN TRANSACTION;
CREATE TABLE IF NOT EXISTS "learnt" (
	"turn_id"	INTEGER,
	"value"	TEXT,
	PRIMARY KEY("turn_id")
);
CREATE TABLE IF NOT EXISTS "proposal" (
	"turn_id"	INTEGER,
	"pid"	INTEGER,
	"seq"	INTEGER,
	"value"	TEXT,
	PRIMARY KEY("turn_id")
);
COMMIT;
