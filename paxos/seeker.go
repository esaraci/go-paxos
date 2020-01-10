// seeker.go introduces a new component to the paxos algorithm.
// The seeker components work together with the proposer in order to achieve consistency and forward progress.
// While the seeker needs the proposer in order to perform his task, the proposer does not need to know about the seeker's existence.
// The task of the seeker is to "seek" missing values be those new, never seen values, or old, never finalized proposals.
// To avoid flooding the network with packets, some probabilities are introduced in the seeker functions, meaning that
// not all missing values might be learnt with just one seeking procedure.
// The seeking procedure is however periodical, this guarantees that, eventually, all values will be learned.

package paxos

import (
	"encoding/json"
	"go-paxos/paxos/config"
	"go-paxos/paxos/messages"
	"go-paxos/paxos/proposal"
	"go-paxos/paxos/queries"
	"log"
	"math/rand"
	"net/http"
	"time"
)

// extractRandomNodes selects (with given probability) a list of nodes.
// This is useful when we dont want to flood the network.
func extractRandomNodes(pr float64) *[]string {

	var nodes []string
	for _, node := range config.CONF.NODES {
		r := rand.Float64()
		if r < pr { // extracting node with a given probability
			//log.Printf("[SEEKER] -> Node %s has been extracted as a target for this seek request.", node)
			nodes = append(nodes, node)
		}
	}

	return &nodes
}

// extractRandomProposals selects (with given probability) the dangling proposals for which a new prepare request will be sent.
// The aim of this function is to reduce the number of the proposals that will be flooding the network.
func extractRandomProposals(danglingProposals *map[int]proposal.Proposal, pr float64) *map[int]proposal.Proposal {

	for turnID := range *danglingProposals {
		r := rand.Float64()
		if r < (1 - pr) { // removing proposals from list with probability 1-p. i.e. "choosing proposals with probability p"
			delete(*danglingProposals, turnID)
		}
	}

	return danglingProposals
}

// SendSeek calls the function askForDanglingProposals and askForNewValues. Both of those aim to achieve eventual consistency.
func SendSeek() {

	log.Print("[SEEKER] -> Seeking procedure is starting now.")
	// these could and should both be goroutines, but it would make it really hard to read logs.
	askForDanglingProposals()
	time.Sleep(2 * time.Second)
	askForNewValues()
	log.Print("[SEEKER] -> Seeking procedure is over.")

}

// askForDanglingProposals will retrieve those proposals whose value is not learnt yet ('dangling' proposals). After doing so a prepare request will be instantiated for each retrieved (dangling) proposal.
// The aim of this function is to achieve forward progress for those proposals which, for any kind of reason, never managed to get learnt by the network.
// This function is the first of the two components (the second being askForNewValues) whose objective is to achieve consistency (safety) which in this case is strictly linked with froward progress.
// This is the only function which needs to know about the existence of the proposer, since its SendPrepare function is used. The proposer however, like the acceptor or the learner, only knows about its own existence.
func askForDanglingProposals() {

	// getting all proposals which dont have an entry in the 'learnt' table
	danglingProposals := queries.GetDanglingProposals()

	// reducing the number of dangling proposals to seek
	// keep in mind that each node will perform this kind of request
	// param pr should be tuned aiming not to overload the network (0.5)
	danglingProposals = extractRandomProposals(danglingProposals, config.CONF.PR_PROPOSALS)

	if len(*danglingProposals) == 0 {
		log.Printf("[SEEKER] -> There are currently no dangling proposals or no proposals have been extracted.")
	}

	// will not enter in for body if danglingProposals has length = 0
	for turnID, danglingProposal := range *danglingProposals {

		log.Printf("[SEEKER] -> Seeking dangling proprosal with turn id %d.", turnID)
		go SendPrepare(turnID, danglingProposal.Seq, danglingProposal.V, config.CONF.OPTIMIZATION)

	}

}

// askForNewValues sends a message to the other nodes containing its last learnt turnID, and a list of turnIDs whose value is not learnt yet.
// If a turnID corresponds to a dangling proposal then that turnID will NOT be inserted in the previously cited list,
// the reason being that danglingProposals will be 'eventually' handled by askForDanglingProposals.
// The reason we send the last turnID is because we want to know if there are some new values (with higher turnID) that never reached us.
func askForNewValues() {
	session := &http.Client{Timeout: time.Second * config.CONF.TIMEOUT}
	// selecting only some nodes, i.e. selecting a node with probability p
	nodes := *extractRandomNodes(config.CONF.PR_NODES)
	log.Printf("[SEEKER] -> %d node(s) has/have been selected as target(s) to seek for new values.", len(nodes))

	if len(nodes) != 0 {
		ch := make(chan []byte, len(nodes))

		// getting last id
		newValuesRequest := ComputeNewValuesRequest()

		for _, node := range nodes {
			url := node + "/seeker/receive_seek"
			go sendPartialRequest(session, url, ch, newValuesRequest)
		}

		checkNewValuesResponses(ch)
	}
}

// learnFromDict learns multiple values from a map with entries in the form of {tunnID: "value"}.
// This function is called after the other nodes have responded to one of our askForNewValues request.
func learnFromDict(newValuesResponses *map[int]string) {
	log.Printf("[SEEKER] -> Learning from merged responses.")
	for turnID, proposedV := range *newValuesResponses {
		currentV := queries.GetLearntValue(turnID)

		if currentV != "" && proposedV != currentV && proposedV != "" {
			log.Printf("[SEEKER] -> !!WARNING!!  im trying to learn a different non empty value")
			// this should never happen
		}
		if currentV == "" && proposedV != "" {
			_ = queries.SetLearntValue(turnID, proposedV)
		}
	}
}

// TODO: change docs, from today 26/12/19 ComputeNewValuesRequest will return and empty "missing" field, i.e. it will only ask for new (higher) values
// TODO: no need to perform any changes when receiving (i.e. inside ComputeNewValuesResponse) since an empty "missing" does not raise any problem
// ComputeNewValuesRequest computes the list of turn ids needed by the askForNewValues function.
// One component of the request is the last turn id learnt (last when sorted, i.e. the highest). The other component is a list of 'missing' turn ids.
// The list is computed starting from 1 and going to the last id. If an element is not found in neither one of the two tables (proposal, learnt) then it's added to the list.
// e.g.
//	Turn IDs in 'proposal' table --> 	P = [1, 2, 5, 6, 10]
//	Turn IDs in 'learnt' table --> 		P = [1, 2, 8]
//
//	The message we send will have lastID = 7, missing = [3, 4, 7]
//
// 5, 6 are not included since they are dangling proposals and will be handled by askForDanglingProposals.
// any value higher than 8 (9, 10) is currently ignored, but if any node has info about any proposal with turnID > 8 it will let us know since we told them what our highest turn id was.
func ComputeNewValuesRequest() messages.NewValuesRequest {

	// highest learnt turn id
	lastID := queries.GetLastTurnID()

	// turn ids of the learnt values
	learntValuesTurnIDs := *queries.GetLearntValuesTurnID()

	// turn ids of the proposals
	proposalsTurnIDs := *queries.GetProposalsTurnID()

	missing := []int{}
	// computing missing list
	// starting from 1 since turn ids start from 1
	for turnID := 1; turnID <= lastID; turnID++ {
		if !learntValuesTurnIDs[turnID] && !proposalsTurnIDs[turnID] {
			// if id is not learnt and is not in proposals
			missing = append(missing, turnID)
		}
		// if turn id is already in learnt then i dont need to learn it
		// if turn id is already in proposals then i dont need to look for it since askForDanglingProposals will do that.
	}

	// TODO: now 'missing' is used to send a new proposal.
	// 'missing' are those TIDs who are not in the 'proposal' nor the 'learnt' table
	// possibly got lost somewhere
	for _, turnID := range missing {
		log.Printf("[SEEKER] -> Seeking dangling proprosal with turn id %d.", turnID)
		go SendPrepare(turnID, 1, "", false)

	}

	// TODO: Missing: missing, but from 26/12/19 this has changed and it is now just an empty slice
	return messages.NewValuesRequest{
		Missing: []int{},
		Last:    lastID,
	}
}

// ComputeNewValuesResponse returns a NewValuesResponse message containing a amp with values to be learned by the requester.
// This function is only triggered when a node sends a NewValuesRequest.
func ComputeNewValuesResponse(newValuesRequest messages.NewValuesRequest) messages.NewValuesResponse {
	toLearn := map[int]string{} // map, in this way i dont need to handle whether keys (turn ids) are unique
	myLast := queries.GetLastTurnID()

	// check if i have something that goes beyond the last learnt turnID of the requester
	if myLast > newValuesRequest.Last {
		log.Printf("[SEEKER] -> I'm ahead of the requester. My last learnt turn id is %d, his is %d.", myLast, newValuesRequest.Last)
		myLearnt := queries.GetAllLearntValues()

		// if so add the to the 'toLearn' map
		// turning list of leartWithIDs into map
		for _, v := range myLearnt {
			if v.TurnID > newValuesRequest.Last {
				// adding element only if the turnID of myLearnt elem is higher than the last turnID learned by the requester
				// i.e. only if the requester hasn't already learnt this value (based on the request he is making)
				log.Printf("[SEEKER] -> Adding [%d, %s] to toLearn since its turn id is higher than the requester's (%d).", v.TurnID, v.Learnt, newValuesRequest.Last)
				toLearn[v.TurnID] = v.Learnt
			}

		}
	}

	// TODO: from 26/12/19 newValuesRequest.Missing will always be empty
	// now toLearn contains all the values (available to me) going from [hisLast:myLast]
	// i should now compute missing. Of course, if seekingRequest.Last was 0 i have already added to toLearn all my known values
	// so i dont need to compute missing again.
	if newValuesRequest.Last != 0 {
		log.Printf("[SEEKER] -> Now addressing the requester's missing values %v.", newValuesRequest.Missing)
		for _, turnID := range newValuesRequest.Missing {

			v := queries.GetLearntValue(turnID)
			if turnID <= myLast && v != "" {
				// if i actually know that value (v != "") and if the requested turn id is not already higher than what i possibly could have (turnID <= myLast)
				log.Printf("[SEEKER] -> Adding [%d, %s] to toLearn since it was requested.", turnID, v)
				toLearn[turnID] = v
			}

		}
	}

	res := messages.NewValuesResponse{ToLearn: toLearn}

	log.Printf("[SEEKER] -> Sending back %v as values to learn.", res)
	return res
}

// checkNewValuesResponses merges the responses received by the newValuesResponses. After merging the responses into a map {turnID: "value"}, the map is then learnt by calling learnFromDict.
// We merge responses into a map so that we dont access the database multiple times for the same turn id.
func checkNewValuesResponses(responseBuffer chan []byte) {

	mergedToLearn := make(map[int]string)
	for i := 0; i < cap(responseBuffer); i++ {
		// popping one message from buffer
		responseData := <-responseBuffer

		// initializing data struct upon which the message will be unmarshalled
		responseMessage := messages.NewValuesResponse{}

		if responseData != nil {
			err := json.Unmarshal(responseData, &responseMessage)
			if err != nil {
				log.Print(err.Error())
			}

			for turnID, v := range responseMessage.ToLearn {
				mergedToLearn[turnID] = v
			}

		}

	}

	if len(mergedToLearn) == 0 {
		log.Print("[SEEKER] -> No new values have been learned from the other nodes.")
	} else {
		log.Printf("[SEEKER] -> Merged responses from nodes. Learning all new values.")
		learnFromDict(&mergedToLearn)
	}

}
