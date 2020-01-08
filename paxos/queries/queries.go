// Package queries implements all the queries needed by this specific implementation of the Paxos algorithm.
package queries

import (
	"go-paxos/paxos/config"
	"go-paxos/paxos/messages"
	"go-paxos/paxos/proposal"
)

// PrepareDBConn initializes the DB
func PrepareDBConn() {
	if config.CONF.DB_TYPE == "sqlite" {
		SQLitePrepareDBConn()
	} else {
		RedisPrepareDBConn()
	}
}

// InitDatabase creates tables and columns. Only used for SQLite
func InitDatabase() {
	SQLiteInitDatabase()
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
	if config.CONF.DB_TYPE == "sqlite" {
		return SQLiteGetProposal(turnID)
	} else {
		return RedisGetProposal(turnID)
	}
}

// GetAllProposals returns a list of all the entries stored in the 'proposal' table.
// Each entry is mapped onto a messages.ProposalWithTid object.
func GetAllProposals() []messages.ProposalWithTid {
	if config.CONF.DB_TYPE == "sqlite" {
		return SQLiteGetAllProposals()
	} else {
		return RedisGetAllProposals()
	}
}

// SetProposal inserts/updates an entry in the 'proposal' table where the field 'turn_id' is equal to @turnID.
// If isAcceptRequest is false, only the value "n" (i.e. Pid and Seq) will be overwritten, while "v" will be left untouched.
// If isAcceptRequest is true, both "v" and "n" will be overwritten by the value requested.
func SetProposal(turnID int, p proposal.Proposal, isAcceptRequest bool) (err error) {
	if config.CONF.DB_TYPE == "sqlite" {
		return SQLiteSetProposal(turnID, p, isAcceptRequest)
	} else {
		return RedisSetProposal(turnID, p, isAcceptRequest)
	}
}

// ResetProposal deletes the entry from the 'proposal' table where the field 'turn_id' is equal to @turnID.
func ResetProposal(turnID int) error {
	if config.CONF.DB_TYPE == "sqlite" {
		return SQLiteResetProposal(turnID)
	} else {
		return RedisResetProposal(turnID)
	}
}

// ResetAllProposals empties the `proposal` table.
func ResetAllProposals() error {
	if config.CONF.DB_TYPE == "sqlite" {
		return SQLiteResetAllProposals()
	} else {
		return RedisResetAllProposals()
	}
}

// GetProposalsTurnID is a map used as a set, the keys are the turnIDs of the proposals we know.
// map[int]interface{} is said to be more efficient than map[int]bool, doesn't really matter.
func GetProposalsTurnID() *map[int]bool {
	if config.CONF.DB_TYPE == "sqlite" {
		return SQLiteGetProposalsTurnID()
	} else {
		return RedisGetProposalsTurnID()
	}
}

// GetDanglingProposals returns a map of the proposals found in the 'proposal' table whose turn ID does not have an entry 'learnt' table.
// The map uses the turn ID as the key and a Proposal object as the value.
func GetDanglingProposals() *map[int]proposal.Proposal {
	if config.CONF.DB_TYPE == "sqlite" {
		return SQLiteGetDanglingProposals()
	} else {
		return RedisGetDanglingProposals()
	}
}

/*
# ========================================================= #
#                   LEARNT VALUE QUERIES                    #
# ========================================================= #
*/

// GetLearntValue returns the 'v' field of the 'learnt' table where the field 'turn_id' is equal to @turnID.
// If no value has been learnt for the requested @turnID, an empty string is returned.
func GetLearntValue(turnID int) string {
	if config.CONF.DB_TYPE == "sqlite" {
		return SQLiteGetLearntValue(turnID)
	} else {
		return RedisGetLearntValue(turnID)
	}
}

// SetLearntValue inserts/updates an entry in the 'learnt' table where the field 'turn_id' is equal to @turnID.
// If the requested @turnID does not exist, a new entry is created.
// If the learnt value for the requested @turnID is already present, it will be overwritten. (why?)
func SetLearntValue(turnID int, v string) (err error) {
	if config.CONF.DB_TYPE == "sqlite" {
		return SQLiteSetLearntValue(turnID, v)
	} else {
		return RedisSetLearntValue(turnID, v)
	}
}

// ResetLearntValue deletes the entry from the 'learnt' table where the field 'turn_id' is equal to @turnID.
func ResetLearntValue(turnID int) error {
	if config.CONF.DB_TYPE == "sqlite" {
		return SQLiteResetLearntValue(turnID)
	} else {
		return RedisResetLearntValue(turnID)
	}
}

// ResetAllLearntValues empties the `learnt` table.
func ResetAllLearntValues() error {
	if config.CONF.DB_TYPE == "sqlite" {
		return SQLiteResetAllLearntValues()
	} else {
		return RedisResetAllLearntValues()
	}
}

// GetAllLearntValues returns a list of all the entries stored in the 'learnt' table.
// Each entry is mapped onto a LearntWithTid object.
func GetAllLearntValues() []messages.LearntWithTid {
	if config.CONF.DB_TYPE == "sqlite" {
		return SQLiteGetAllLearntValues()
	} else {
		return RedisGetAllLearntValues()
	}
}

// GetLastTurnID returns the highest turn ID found in the `learnt` table.
// 0 is returned if table is empty.
func GetLastTurnID() int {
	if config.CONF.DB_TYPE == "sqlite" {
		return SQLiteGetLastTurnID()
	} else {
		return RedisGetLastTurnID()
	}
}

// GetLearntValuesTurnID is a map used as a set, the keys are the turnIDs of the learnt values.
// map[int]interface{} is said to be more efficient than map[int]bool, doesn't really matter.
func GetLearntValuesTurnID() *map[int]bool {
	if config.CONF.DB_TYPE == "sqlite" {
		return SQLiteGetLearntValuesTurnID()
	} else {
		return RedisGetLearntValuesTurnID()
	}
}
