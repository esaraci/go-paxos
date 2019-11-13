// Package queries implements all the queries needed by this specific implementation of the Paxos algorithm.
package queries

import (
	"database/sql"
	_ "github.com/mattn/go-sqlite3" // blank import because of no explicit use, only side effects needed.
	"go-paxos/paxos/config"
	"go-paxos/paxos/messages"
	"go-paxos/paxos/proposal"
	"log"
)

const (
	sqlDriver = "sqlite3"
)

var db *sql.DB

func PrepareDBConn() {
	db, _ = sql.Open(sqlDriver, config.CONF.DB_PATH)
}

// InitDatabase executes the command needed to initialize the database.
func InitDatabase() {
	_, _ = db.Exec(`BEGIN TRANSACTION;
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
	COMMIT;`)
}

/*
# ========================================================= #
#                     PROPOSAL QUERIES                      #
# ========================================================= #
*/

// GetProposal returns the entry of the 'proposal' table where the field 'turn_id' is equal to @turnID.
// If the wanted proposal does not exist OR is not valid (i.e. pid and seq are 0)
// an empty proposal is returned together with a false boolean value.
// Please note that proposals with @pid or @seq = 0 should not exist, the user should not issue such values.
// If the wanted proposal exists AND is valid, it will be returned together with a positive boolean value.
// If the field 'v' is NULL, @v will be assigned the empty string "".
// The entry will be mapped onto a proposal.Proposal object.
func GetProposal(turnID int) (proposal.Proposal, bool) {

	//db, _ := sql.Open(sqlDriver, config.CONF.DB_PATH)
	row := db.QueryRow("SELECT pid, seq, value FROM proposal WHERE turn_id = ?", turnID)

	// sql.NullInt64, sql.NullString are "NULL-accepting" types
	var pid sql.NullInt64
	var seq sql.NullInt64
	var v sql.NullString

	err := row.Scan(&pid, &seq, &v)
	if err != nil {
		// sql.ErrNoRows
		log.Printf("[QUERIES] -> no proposal found for turn id: %d; returning an empty proposal.", turnID)
	}

	ok := false
	p := proposal.Proposal{}

	if pid.Valid && seq.Valid {
		// both pid and seq are not 0
		ok = true
		p = proposal.Proposal{Pid: int(pid.Int64), Seq: int(seq.Int64), V: v.String}
	}

	// if saved proposal is invalid then p is empty and ok is false
	// otherwise p is the saved proposal and ok is true
	return p, ok
}

// GetAllProposals returns a list of all the entries stored in the 'proposal' table.
// Each entry is mapped onto a messages.ProposalWithTid object.
func GetAllProposals() []messages.ProposalWithTid {

	//db, _ := sql.Open(sqlDriver, config.CONF.DB_PATH)
	rows, err := db.Query("SELECT * FROM proposal ORDER BY turn_id")
	if err != nil {
		log.Print("ERR rilevato in db.Query - ", err.Error())
	}

	var m []messages.ProposalWithTid

	defer rows.Close()
	for rows.Next() {

		var turnID int
		var pid sql.NullInt64
		var seq sql.NullInt64
		var v sql.NullString

		err := rows.Scan(&turnID, &pid, &seq, &v)

		if err != nil {
			log.Print("Error while scanning values: ", err.Error())
		}

		p := proposal.Proposal{Pid: int(pid.Int64), Seq: int(seq.Int64), V: v.String}
		m = append(m, messages.ProposalWithTid{TurnID: turnID, Proposal: p})

	}
	return m
}

// SetProposal inserts/updates an entry in the 'proposal' table where the field 'turn_id' is equal to @turnID.
// If isAcceptRequest is false, only the value "n" (i.e. Pid and Seq) will be overwritten, while "v" will be left untouched.
// If isAcceptRequest is true, both "v" and "n" will be overwritten by the value requested.
func SetProposal(turnID int, p proposal.Proposal, isAcceptRequest bool) (err error) {

	//db, _ := sql.Open(sqlDriver, config.CONF.DB_PATH)
	if p.V != "" {
		if isAcceptRequest {
			// is accept request
			_, err = db.Exec("INSERT INTO proposal VALUES(?, ?, ?, ?) ON CONFLICT (turn_id) DO UPDATE SET pid = excluded.pid, seq = excluded.seq, value = excluded.value", turnID, p.Pid, p.Seq, p.V)
		} else {
			// is prepare request with non empty V. If the stored value is not NULL it will not be overwritten.
			// coalesce returns the first non null argument passed to it.
			_, err = db.Exec("INSERT INTO proposal VALUES(?, ?, ?, ?) ON CONFLICT (turn_id) DO UPDATE SET pid = excluded.pid, seq = excluded.seq, value = coalesce(value, excluded.value)", turnID, p.Pid, p.Seq, p.V)
		}

	} else {
		// this can only be a prepare request, V is always non empty in accept requests
		// this query prevents emptystring to be saved as V
		_, err = db.Exec("INSERT INTO proposal VALUES(?, ?, ?, NULL) ON CONFLICT (turn_id) DO UPDATE SET pid = excluded.pid, seq = excluded.seq", turnID, p.Pid, p.Seq)

	}
	return err
}

// ResetProposal deletes the entry from the 'proposal' table where the field 'turn_id' is equal to @turnID.
func ResetProposal(turnID int) error {
	//db, _ := sql.Open(sqlDriver, config.CONF.DB_PATH)
	_, err := db.Exec("DELETE FROM proposal WHERE turn_id = ?", turnID)
	return err
}

// ResetAllProposals empties the `proposal` table.
func ResetAllProposals() error {
	//db, _ := sql.Open(sqlDriver, config.CONF.DB_PATH)
	_, err := db.Exec("DELETE FROM proposal")
	return err
}

// GetProposalsTurnID is a map used as a set, the keys are the turnIDs of the proposals we know.
// map[int]interface{} is said to be more efficient than map[int]bool, doesn't really matter.
func GetProposalsTurnID() *map[int]bool {
	//db, _ := sql.Open(sqlDriver, config.CONF.DB_PATH)
	rows, _ := db.Query("SELECT turn_id FROM proposal ORDER BY turn_id ASC")

	proposalsTurnID := make(map[int]bool)

	defer rows.Close()
	for rows.Next() {
		var turnID int
		err := rows.Scan(&turnID)
		if err != nil {
			log.Print("scanning into  turn_id failed: ", err.Error())
		} else {
			proposalsTurnID[turnID] = true
		}

	}
	return &proposalsTurnID
}

// GetDanglingProposals returns a map of the proposals found in the 'proposal' table whose turn ID does not have an entry 'learnt' table.
// The map uses the turn ID as the key and a Proposal object as the value.
func GetDanglingProposals() *map[int]proposal.Proposal {

	//db, _ := sql.Open(sqlDriver, config.CONF.DB_PATH)
	rows, err := db.Query("SELECT p.turn_id, p.pid, p.seq, p.value FROM proposal as p LEFT JOIN learnt as l ON p.turn_id = l.turn_id WHERE l.turn_id is NULL")
	if err != nil {
		log.Print("ERR rilevato in db.Query - ", err.Error())
	}

	danglingProposals := make(map[int]proposal.Proposal)

	defer rows.Close()
	for rows.Next() {
		var turnID int
		var pid int
		var seq int
		var v sql.NullString
		err := rows.Scan(&turnID, &pid, &seq, &v)
		if err != nil {
			log.Print("scanning into  turn_id failed: ", err.Error())
		} else {
			danglingProposals[turnID] = proposal.Proposal{Pid: pid, Seq: seq, V: v.String}
		}

	}

	return &danglingProposals
}

/*
# ========================================================= #
#                   LEARNT VALUE QUERIES                    #
# ========================================================= #
*/

// GetLearntValue returns the 'v' field of the 'learnt' table where the field 'turn_id' is equal to @turnID.
// If no value has been learnt for the requested @turnID, an empty string is returned.
func GetLearntValue(turnID int) string {
	//db, _ := sql.Open(sqlDriver, config.CONF.DB_PATH)
	row := db.QueryRow("SELECT value FROM learnt WHERE turn_id = ?", turnID)

	var v sql.NullString
	err := row.Scan(&v)
	if err != nil {
		// sql.ErrNoRows
		log.Printf("[QUERIES] -> No learnt value found for turn_id: %d; keep going.", turnID)
	}
	return v.String
}

// SetLearntValue inserts/updates an entry in the 'learnt' table where the field 'turn_id' is equal to @turnID.
// If the requested @turnID does not exist, a new entry is created.
// If the learnt value for the requested @turnID is already present, it will be overwritten.
func SetLearntValue(turnID int, v string) (err error) {
	//db, _ := sql.Open(sqlDriver, config.CONF.DB_PATH)
	_, err = db.Exec("INSERT INTO learnt VALUES(?, ?) ON CONFLICT (turn_id) DO UPDATE SET value = excluded.value", turnID, v)

	return err
}

// ResetLearntValue deletes the entry from the 'learnt' table where the field 'turn_id' is equal to @turnID.
func ResetLearntValue(turnID int) error {
	//db, _ := sql.Open(sqlDriver, config.CONF.DB_PATH)
	_, err := db.Exec("DELETE FROM learnt WHERE turn_id = ?", turnID)

	return err
}

// ResetAllLearntValues empties the `learnt` table.
func ResetAllLearntValues() error {
	//db, _ := sql.Open(sqlDriver, config.CONF.DB_PATH)
	_, err := db.Exec("DELETE FROM learnt")
	return err
}

// GetAllLearntValues returns a list of all the entries stored in the 'learnt' table.
// Each entry is mapped onto a LearntWithTid object.
func GetAllLearntValues() []messages.LearntWithTid {
	//db, _ := sql.Open(sqlDriver, config.CONF.DB_PATH)
	rows, err := db.Query("SELECT * FROM learnt ORDER BY turn_id")
	if err != nil {
		log.Print("ERR rilevato in db.Query - ", err.Error())
	}

	var m []messages.LearntWithTid

	defer rows.Close()
	for rows.Next() {

		var turnID int
		var v sql.NullString

		err := rows.Scan(&turnID, &v)

		if err != nil {
			log.Print("scanning into  turn_id", err.Error())
		}

		m = append(m, messages.LearntWithTid{TurnID: turnID, Learnt: v.String})

	}
	return m
}

// GetMissingTurnIDs returns a list of turnIDs that are present in the 'proposal' table but not in the 'learnt' table.
// This function not used anymore
/*
func GetMissingTurnIDs() []int {
	//db, _ := sql.Open(sqlDriver, config.CONF.DB_PATH)
	rows, err := db.Query("SELECT l.turn_id FROM learnt as l LEFT JOIN proposal as p ON l.turn_id = p.turn_id WHERE p.turn_id is NULL")
	if err != nil {
		log.Print("ERR rilevato in db.Query - ", err.Error())
	}

	var missing []int

	defer rows.Close()
	for rows.Next() {
		var turnID int
		err := rows.Scan(&turnID)
		if err != nil {
			log.Print("scanning into  turn_id failed: ", err.Error())
		}
		missing = append(missing, turnID)
	}
	return missing
}
*/

// GetLastTurnID returns the highest turn ID found in the `learnt` table.
// 0 is returned if table is empty.
func GetLastTurnID() int {
	//db, _ := sql.Open(sqlDriver, config.CONF.DB_PATH)
	row := db.QueryRow("SELECT turn_id FROM learnt ORDER BY turn_id DESC")

	var lastID int

	err := row.Scan(&lastID)
	if err != nil {
		// sql.ErrNoRows, --> problema nel nome del campo/tabella oppure problemi nella dbessione
	}
	return lastID
}

// GetLearntValuesTurnID is a map used as a set, the keys are the turnIDs of the learnt values.
// map[int]interface{} is said to be more efficient than map[int]bool, doesn't really matter.
func GetLearntValuesTurnID() *map[int]bool {
	//db, _ := sql.Open(sqlDriver, config.CONF.DB_PATH)
	rows, _ := db.Query("SELECT turn_id FROM learnt ORDER BY turn_id ASC")

	learntValuesTurnID := make(map[int]bool)

	defer rows.Close()
	for rows.Next() {
		var TurnID int
		err := rows.Scan(&TurnID)
		if err != nil {
			log.Print("scanning into  turn_id failed: ", err.Error())
		} else {
			learntValuesTurnID[TurnID] = true
		}

	}
	return &learntValuesTurnID
}
