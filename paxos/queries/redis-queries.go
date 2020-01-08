// Package queries implements all the queries needed by this specific implementation of the Paxos algorithm.
package queries

import (
	"fmt"
	"github.com/go-redis/redis/v7"
	"go-paxos/paxos/config"
	"go-paxos/paxos/messages"
	"go-paxos/paxos/proposal"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

// util
func proposalStringToProposal(pS string) (int, proposal.Proposal) {
	components := strings.Split(pS, ":")

	turn_id, _ := strconv.Atoi(components[0])
	pid, _ := strconv.Atoi(components[1])
	seq, _ := strconv.Atoi(components[2])
	v := components[3]

	return turn_id, proposal.Proposal{
		Pid: pid,
		Seq: seq,
		V:   v,
	}
}

// util
func learntStringToLearnt(pS string) (int, string) {
	components := strings.Split(pS, ":")

	turn_id, _ := strconv.Atoi(components[0])
	v := components[1]

	return turn_id, v
}

var client *redis.Client

func RedisPrepareDBConn() {
	client = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})
	_, err := client.Ping().Result()
	if err != nil {
		log.Fatal("Redis server did not PONG back to our PING")
	}
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
func RedisGetProposal(turnID int) (proposal.Proposal, bool) {

	rKey := fmt.Sprintf("proposal:%d", turnID)
	proposalString, err := client.Get(rKey).Result()

	p := proposal.Proposal{}
	ok := false

	if err != nil || err == redis.Nil {
		// an error occurred when reading or no proposal found
		log.Printf("[QUERIES] -> No proposal found for turn id: %d; returning an empty proposal.", turnID)

	} else {
		// assert rkey is memeber of proposals set
		// redis: this is debug info
		res := client.SIsMember("proposals", turnID).Val()
		if res != true {
			panic("false assertion, proposal was found on proposal table but not on proposal set. only the opposite can be true")
		}

		_, p = proposalStringToProposal(proposalString)
		ok = true
	}

	return p, ok

}

// GetAllProposals returns a list of all the entries stored in the 'proposal' table.
// Each entry is mapped onto a messages.ProposalWithTid object.
func RedisGetAllProposals() []messages.ProposalWithTid {

	var m []messages.ProposalWithTid

	tids, err := client.SMembers("proposals").Result()
	if err != nil {
		log.Print("ERR rilevato in client.SMembers - ", err.Error())
	} else {

		sort.Strings(tids)
		// iterating through keys and retrieving values
		for _, tid := range tids {
			// for each key, extract turn id
			tid, err := strconv.Atoi(tid)
			if err != nil {
				log.Print("Error when converting keys: ", err.Error())
			} else {
				// successfully extracted and converted turn id, now get the proposal
				p, ok := RedisGetProposal(tid)
				if ok {
					m = append(m, messages.ProposalWithTid{TurnID: tid, Proposal: p})
				}
			}
		}
	}

	return m
}

// SetProposal inserts/updates an entry in the 'proposal' table where the field 'turn_id' is equal to @turnID.
// If isAcceptRequest is false, only the value "n" (i.e. Pid and Seq) will be overwritten, while "v" will be left untouched.
// If isAcceptRequest is true, both "v" and "n" will be overwritten by the value requested.
func RedisSetProposal(turnID int, p proposal.Proposal, isAcceptRequest bool) (err error) {

	// adding proposals to set
	rKey := fmt.Sprintf("proposal:%d", turnID)
	client.SAdd("proposals", turnID)
	// if node fails now it does not matter, the proposal will be empty and everything will work as intended

	if isAcceptRequest {
		// is accept request, overwrite everything
		rVal := fmt.Sprintf("%d:%d:%d:%s", turnID, p.Pid, p.Seq, p.V)
		_, err := client.Set(rKey, rVal, 0).Result()

		return err

	} else {
		// is prepare request with non empty V. If the stored value is not NULL it will not be overwritten.
		err := client.Watch(func(tx *redis.Tx) error {
			proposalString, err := tx.Get(rKey).Result()

			// if the proposal actually exists
			if proposalString != "" {
				_, currentP := proposalStringToProposal(proposalString)
				if currentP.V != "" {
					// se il V attuale non è nullo, non lo sovrascrivo (i.e. lo sovrascrivo con se stesso)
					p.V = currentP.V
				}
			}

			rVal := fmt.Sprintf("%d:%d:%d:%s", turnID, p.Pid, p.Seq, p.V)
			_, err = tx.Pipelined(func(pipe redis.Pipeliner) error {
				pipe.Set(rKey, rVal, 0)
				return nil
			})
			return err
		}, rKey)

		return err
	}

}

// ResetProposal deletes the entry from the 'proposal' table where the field 'turn_id' is equal to @turnID.
func RedisResetProposal(turnID int) error {
	rKey := fmt.Sprintf("proposal:%d", turnID)

	// pipeline
	pipe := client.TxPipeline()
	pipe.SRem("proposals", strconv.Itoa(turnID))
	pipe.Del(rKey)
	_, err := pipe.Exec()

	// executes multiple actions atomically
	return err
}

// ResetAllProposals empties the `proposal` table.
func RedisResetAllProposals() error {
	tids, err := client.SMembers("proposals").Result()
	if err != nil {
		log.Print("ERR rilevato in client.SMembers - ", err.Error())
	} else {
		// iterating through keys and retrieving values
		for _, tid := range tids {

			//now call reset proposal
			tid, err := strconv.Atoi(tid)
			if err != nil {
				log.Print("Error when converting keys: ", err.Error())
			} else {
				err = RedisResetProposal(tid)
				if err != nil {
					return err
				}
			}
		}
	}
	return err
}

// GetProposalsTurnID is a map used as a set, the keys are the turnIDs of the proposals we know.
// map[int]interface{} is said to be more efficient than map[int]bool, doesn't really matter.
func RedisGetProposalsTurnID() *map[int]bool {
	proposalsTurnID := make(map[int]bool)

	//db, _ := sql.Open(sqlDriver, config.CONF.DB_PATH)
	tids, err := client.SMembers("proposals").Result()
	if err != nil {
		log.Print("ERR rilevato in client.SMembers - ", err.Error())
	} else {

		for _, tid := range tids {
			// for each key, extract turn id
			tid, err := strconv.Atoi(tid)
			if err != nil {
				log.Print("Error when converting keys: ", err.Error())
			} else {
				// successfully extracted and converted turn id, now add it to the map
				proposalsTurnID[tid] = true
			}
		}
	}
	return &proposalsTurnID
}

// GetDanglingProposals returns a map of the proposals found in the 'proposal' table whose turn ID does not have an entry 'learnt' table.
// The map uses the turn ID as the key and a Proposal object as the value.
func RedisGetDanglingProposals() *map[int]proposal.Proposal {

	danglingProposals := make(map[int]proposal.Proposal)

	danglingTids, err := client.SDiff("proposals", "learnt").Result()
	if err != nil {
		fmt.Printf("Error when computing SDiff between proposals and learnt")
	}

	for _, dTid := range danglingTids {
		tid, err := strconv.Atoi(dTid)
		p, ok := GetProposal(tid)

		if err != nil || !ok {
			fmt.Printf("Error when converting tid or when retrieving proposal by tid")
		} else {
			danglingProposals[tid] = p
		}
	}

	//danglingProposals[turnID] = proposal.Proposal{Pid: pid, Seq: seq, V: v.String}
	return &danglingProposals
}

/*
# ========================================================= #
#                   LEARNT VALUE QUERIES                    #
# ========================================================= #
*/

// GetLearntValue returns the 'v' field of the 'learnt' table where the field 'turn_id' is equal to @turnID.
// If no value has been learnt for the requested @turnID, an empty string is returned.
func RedisGetLearntValue(turnID int) string {

	var vString string
	rKey := fmt.Sprintf("learnt:%d", turnID)
	learntString, err := client.Get(rKey).Result()

	if err != nil || err == redis.Nil {
		log.Printf("[QUERIES] -> No learnt value found for turn_id: %d.", turnID)
	} else {
		_, vString = learntStringToLearnt(learntString)
	}
	return vString
}

// SetLearntValue inserts/updates an entry in the 'learnt' table where the field 'turn_id' is equal to @turnID.
// If the requested @turnID does not exist, a new entry is created.
// If the learnt value for the requested @turnID is already present, it will be overwritten. (why?)
func RedisSetLearntValue(turnID int, v string) (err error) {

	rKey := fmt.Sprintf("learnt:%d", turnID)
	rVal := fmt.Sprintf("%d:%s", turnID, v)
	_, err = client.Set(rKey, rVal, 0).Result()

	if err != nil {
		return err
	}
	// pipeline
	pipe := client.TxPipeline()
	pipe.SAdd("learnt", turnID)
	pipe.Set(rKey, rVal, 0)
	_, err = pipe.Exec()

	// counting how many rows in learnt table so i can notify some listener that i learnt all turn_ids
	// it is needed for testing and benchmarking purposes
	card, err := client.SCard("learnt").Result()
	howMany := int(card)
	if err != nil {
		// do nothing
	} else {
		if howMany == config.CONF.NUMBER_OF_TIDS {
			now := time.Now()
			sec := now.Unix()
			go func() {
				_, err := http.Get(fmt.Sprintf("%s/timer?nid=%d&timestamp=%d&how_many=%d", config.CONF.LISTENER_IP, config.CONF.PID, sec, howMany))
				if err != nil {
					log.Printf("Errore nella richiesta di salvataggio del timer: %v", err.Error())
				}
			}()
		}
	}

	return err
}

// ResetLearntValue deletes the entry from the 'learnt' table where the field 'turn_id' is equal to @turnID.
func RedisResetLearntValue(turnID int) error {
	rKey := fmt.Sprintf("learnt:%d", turnID)

	// pipeline
	pipe := client.TxPipeline()
	pipe.SRem("learnt", strconv.Itoa(turnID))
	pipe.Del(rKey)
	_, err := pipe.Exec()

	// executes multiple actions atomically
	return err
}

// ResetAllLearntValues empties the `learnt` table.
func RedisResetAllLearntValues() error {
	tids, err := client.SMembers("learnt").Result()
	if err != nil {
		log.Print("ERR rilevato in SMembers - ", err.Error())
	} else {
		for _, tid := range tids {
			tid, err := strconv.Atoi(tid)
			if err != nil {
				log.Print("Error when converting keys: ", err.Error())
			} else {
				// successfully extracted and converted turn id
				err = RedisResetLearntValue(tid)
				if err != nil {
					return err
				}
			}
		}
	}
	return err
}

// GetAllLearntValues returns a list of all the entries stored in the 'learnt' table.
// Each entry is mapped onto a LearntWithTid object.
func RedisGetAllLearntValues() []messages.LearntWithTid {

	var m []messages.LearntWithTid

	tids, err := client.SMembers("learnt").Result()
	if err != nil {
		log.Print("ERR rilevato in db.Query - ", err.Error())
	} else {
		sort.Strings(tids)
		for _, tid := range tids {

			tid, err := strconv.Atoi(tid)
			if err != nil {
				log.Print("Error when converting keys: ", err.Error())
			} else {
				// successfully extracted and converted turn id
				v := RedisGetLearntValue(tid)
				m = append(m, messages.LearntWithTid{TurnID: tid, Learnt: v})
			}
		}
	}
	return m
}

// GetLastTurnID returns the highest turn ID found in the `learnt` table.
// 0 is returned if table is empty.
func RedisGetLastTurnID() int {
	//row := db.QueryRow("SELECT turn_id FROM learnt ORDER BY turn_id DESC")

	tids := client.SMembers("learnt").Val()

	var lastID int

	for _, v := range tids {
		tid, err := strconv.Atoi(v)
		if err != nil {
			panic(err)
		}
		if tid > lastID {
			lastID = tid
		}
	}
	return lastID
}

// GetLearntValuesTurnID is a map used as a set, the keys are the turnIDs of the learnt values.
// map[int]interface{} is said to be more efficient than map[int]bool, doesn't really matter.
func RedisGetLearntValuesTurnID() *map[int]bool {

	learntValuesTurnID := make(map[int]bool)

	tids, err := client.SMembers("learnt").Result()

	if err != nil {
		log.Print("ERR rilevato in SMembers - ", err.Error())
	} else {
		for _, tid := range tids {
			// for each key, extract turn id
			tid, err := strconv.Atoi(tid)
			if err != nil {
				log.Print("Error when converting keys: ", err.Error())
			} else {
				// successfully extracted and converted turn id, now add it to the map
				learntValuesTurnID[tid] = true
			}
		}
	}
	return &learntValuesTurnID
}
