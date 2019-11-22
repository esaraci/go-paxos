/*

# SendPrepare(n):
1. A proposer chooses a new messages numbered n and sends a request to
each member of some set of acceptors, asking it to respond with:

	(a) A promise never again to accept a messages numbered less than n;
	---AND---
	(b) The messages with the highest number less than n that it has
	accepted, if any.


# SendAccept(n, v):
2. If the proposer receives the requested responses from a majority of
the acceptors, then it can issue a messages with number n and value
v, where v is the value of the highest-numbered messages among the
responses, or is any value selected by the proposer if the responders
reported no proposals.

*/

package paxos

import (
	"encoding/json"
	"fmt"
	"go-paxos/paxos/config"
	"go-paxos/paxos/messages"
	"go-paxos/paxos/proposal"
	"go-paxos/paxos/queries"
	"log"
	"math/rand"
	"net/http"
	"time"
)

// learnAndFlood learns a value and then floods the network with learn requests for that value.
// If the current learnt value (@currentV) is empty the learner learns the proposed value (@proposedV) and floods the network with it.
// Else if the current learnt value (@currentV) is NOT empty, two things can happen:
// 1. @currentV == @proposedV, in this case no further action is performed; we already knew that value.
// 2. @currentV != @proposedV, a warning is printed; some node (or user) is not following the protocol.
// This function is called whenever the field 'Learnt' on a response message during the prepare/accept phase is not empty.
// As soon as such thing occurs the prepare/accept phase is dropped immediately and the proposed value is learnt.
func learnAndFlood(responseMessage messages.GenericMessage) {
	turnID := responseMessage.TurnID
	currentV := queries.GetLearntValue(turnID)
	proposedV := responseMessage.Body.Learnt

	if currentV == "" {
		// i currently dont have a learnt  value for this turnID
		// therefore i should store the value reported in 'learnt', and notify all the other nodes
		// finally i should drop any further computation
		err := queries.SetLearntValue(turnID, proposedV)
		if err != nil {
			// can this ever happen?, yes it can.
			// could not store learnt, do nothing
		} else {
			// flooding with learn requests
			log.Print("[PROPOSER] -> Flooding is about to begin.")
			go SendLearn(turnID, proposedV)
		}


	} else {
		if currentV != proposedV {
			// this is supposed to be deadcode in production
			// this can only happen if we forcefully try to make it happen using "debug" routes while not
			// respecting the algorithm
			log.Print("!!!WARNING!!! BEING PROPOSED TO LEARN A VALUE DIFFERENT FROM WHAT I ALREADY HAVE, are you following the algorithm?")
		}
	}
}

// countAgreements counts how many of the acceptors gave us a 'promise' to our prepare request. Based on the number of responses and their content different actions will be performed.
func countAgreements(responseBuffer chan []byte, turnID int, seq int, proposedV string) (messageToUser string, err error) {
	agreements := 0
	responseCount := 0
	highestPromise := proposal.Proposal{}
	highestRetry := proposal.Proposal{}
	messageToUser = ""

	// for each response collected
	for i := 0; i < cap(responseBuffer); i++ {

		// popping one message from buffer --> previously called "resMsg"
		responseData := <-responseBuffer

		// initializing data struct upon which the message will be unmarshalled --> previously called "res"
		responseMessage := messages.GenericMessage{}

		// @responseData might be nil
		// this happens when a node does not respond in time (e.g. if it's turned off)
		//
		// if @responseData is nil we do nothing,
		// @responseMessage will be an empty message and will not be counted as a "promise"
		//
		// if @responseData is NOT nil, then we unmarshal @responseData over to @responseMessage
		//
		// if errors occur when unmarshalling responses a 0 count agreements is returned for safety
		if responseData != nil {
			//fmt.Printf(string(responseData))
			err := json.Unmarshal(responseData, &responseMessage)
			if err != nil {
				log.Print(err.Error())
				return "Errors while unmarshalling responses, someone is not respecting the protocol.", err
			}
			// no errors during unmarshalling, counting non empty responses
			responseCount++
		}

		// handling "learnt" response
		if ResponseHasLearntValue(responseMessage) {
			log.Printf("[PROPOSER] -> One of the responses has already learnt '%v' for turn id %d. Learn the value and drop any further computation.", responseMessage.Body.Learnt, turnID)
			learnAndFlood(responseMessage)
			return "One of the responses has a learnt value. Learning and flooding.", nil
		}

		// counting promises and saving the highest messages with a value, and the highest retry pid and seq
		if responseMessage.Body.Message == "promise" {
			agreements += 1

			// highest holds the highest non null valued promise response
			prop := responseMessage.Body.Proposal
			if prop.IsGreaterThan(&highestPromise) && prop.V != "" {
				highestPromise = prop
			}

		} else if responseMessage.Body.Message == "retry" {
			prop := responseMessage.Body.Proposal
			if prop.IsGreaterThan(&highestRetry) {
				highestRetry = prop
			}
		}
	}

	// after i checked ALL the proposals (looking for the highest)
	// i check if QUORUM is reached
	if agreements >= config.CONF.QUORUM {

		// QUORUM has been reached
		log.Printf("[PROPOSER] -> Quorum has been reached (%d/%d) for prepare request with proposal {turn_id: %d, seq: %d, v: %s}.", agreements, len(config.CONF.NODES), turnID, seq, proposedV)
		messageToUser = fmt.Sprintf("Quorum has been reached (%d/%d) for prepare request with proposal {turn_id: %d, seq: %d, v: %s}.", agreements, len(config.CONF.NODES), turnID, seq, proposedV)

		// sanity check: has highest ever been updated?
		if highestPromise.V == "" {
			// all responses had empty valued proposals, i can put my own value in there, either proposedV or V_DEFAULT.
			if proposedV == "" { // if user never issued a value for the prepare request i can put a default value
				proposedV = config.CONF.V_DEFAULT
			}
			highestPromise = proposal.Proposal{
				V: proposedV,
			}
		}

		// if any response had a non empty valued proposals, then highestsPromise.V holds that value
		// however i need to assign my "n" to that proposal (i.e. overwriting Pid and Seq) why? <- because mine is higher as I am inside this function
		highestPromise.Pid = config.CONF.PID
		highestPromise.Seq = seq

		// let SendAccept on his own and return
		if !config.CONF.MANUAL_MODE {
			time.Sleep(config.CONF.WAIT_BEFORE_AUTOMATIC_REQUEST * time.Second)
			log.Printf("[PROPOSER] -> Sending accept request.")
			messageToUser += fmt.Sprintf(" Sending accept request.")
			go SendAccept(turnID, highestPromise.Seq, highestPromise.V)
		} else {
			log.Printf("[PROPOSER] -> Waiting for user to send accept request; the algorithm suggests: /proposer/send_accept?turn_id=%d&seq=%d&v=%s", turnID, highestPromise.Seq, highestPromise.V)
			messageToUser += fmt.Sprintf(" Please send an accept request as follows:"+
				" /proposer/send_accept?turn_id=%d&seq=%d&v=%s", turnID, highestPromise.Seq, highestPromise.V)
		}

	} else {
		messageToUser = fmt.Sprintf("Quorum has NOT been reached  (%d/%d) for prepare request with proposal {turn_id: %d, seq: %d, v: %s}.", agreements, len(config.CONF.NODES), turnID, seq, proposedV)
		if highestRetry.Pid != 0 && responseCount >= config.CONF.QUORUM {
			// highestRetry.Pid != 0 is how i check if the highestRetry has ever been updated.
			log.Printf("[PROPOSER] -> Quorum has NOT been reached (%d/%d) for prepare request with proposal {turn_id: %d, seq: %d, v: %s}, but a majority of nodes is up and running; increment 'seq' and retry.", agreements, len(config.CONF.NODES), turnID, seq, proposedV)
			incrementedSeq := highestRetry.Seq + 1
			if !config.CONF.MANUAL_MODE {
				// waiting a random amount before retrying to allow others to finish
				// rand.float32() generates a number between 0 and 1, adding a flat 0.2 amount
				// r := min + rand.Float64() * (max - min)
				r := rand.Float64() * 5
				time.Sleep(time.Duration(r) * time.Second)
				//time.Sleep(config.CONF.WAIT_BEFORE_AUTOMATIC_REQUEST * time.Second)
				log.Printf("[PROPOSER] -> Sending incremented prepare request.")
				messageToUser += fmt.Sprintf(" Retrying with an incrememented prepare request.")
				go SendPrepare(turnID, incrementedSeq, proposedV)
			} else {
				log.Printf("[PROPOSER] -> Waiting for user to retry the prepare request; the algorithm suggests: /proposer/send_prepare?turn_id=%d&seq=%d&v=%s", turnID, incrementedSeq, proposedV)
				messageToUser += fmt.Sprintf(" Please retry with a higher prepare request as follows:"+
					" /proposer/send_prepare?turn_id=%d&seq=%d&v=%s",
					turnID, incrementedSeq, proposedV)
			}
		} else {
			log.Printf("[PROPOSER] -> Quorum has NOT been reached (%d/%d) for prepare request with proposal {turn_id: %d, seq: %d, v: %s}; the algorithm suggests: do not proceed further, progress is not possible.", agreements, len(config.CONF.NODES), turnID, seq, proposedV)
			messageToUser += fmt.Sprintf(" Only %d responded but %d are needed for progress.", responseCount, config.CONF.QUORUM)
		}
	}
	// return agreements even if QUORUM was not reached, not required.
	return messageToUser, nil
}

// countApprovals counts how many of the acceptors gave us an 'accept' to our accept request. Based on the number of responses and their content different actions will be performed.
func countApprovals(responseBuffer chan []byte, turnID int, _ int, proposedV string) (messageToUser string, err error) {
	approvals := 0
	responseCount := 0
	highestDecline := proposal.Proposal{}
	messageToUser = ""

	// for each response collected
	for i := 0; i < cap(responseBuffer); i++ {

		// popping one message from buffer
		responseData := <-responseBuffer

		// initializing data struct upon which the message will be unmarshalled
		responseMessage := messages.GenericMessage{}

		// @responseData might be nil
		// this happens when a node did not respond in time (or is turned off)
		//
		// if @responseData is nil we do nothing,
		// @responseMessage will be an empty message and will not be counted as an "accept"
		//
		// if @responseData is NOT nil, then we unmarshal @responseData over to @responseMessage
		//
		// if error occur unmarshalling responses an error message is returned
		if responseData != nil {
			err := json.Unmarshal(responseData, &responseMessage)
			if err != nil {
				log.Print(err.Error())
				return "Errors while unmarshalling responses", err
			}
			responseCount++
		}

		if ResponseHasLearntValue(responseMessage) {
			log.Printf("[PROPOSER] -> One of the responses has already learnt %v for turn id %d. Learn the value and drop any further computation.", responseMessage.Body.Learnt, turnID)
			learnAndFlood(responseMessage)
			return "One of the responses has a learnt value. Learning and flooding.", nil
		}

		// counting approvals
		if responseMessage.Body.Message == "accept" {
			approvals += 1
		} else if responseMessage.Body.Message == "decline" {
			prop := responseMessage.Body.Proposal

			if prop.IsGreaterThan(&highestDecline) {
				highestDecline = prop
			}
		}
	}

	// i could put this right after the approval increment and break the loop
	// when quorum is reached, but i prefer
	// checking at the end for readability purposes
	if approvals >= config.CONF.QUORUM {
		log.Printf("[PROPOSER] -> Quorum for accept request reached: got %d/%d accepts.", approvals, len(config.CONF.NODES))
		messageToUser = fmt.Sprintf("Quorum has been reached for accept request (%d/%d). ", approvals, len(config.CONF.NODES))
		if !config.CONF.MANUAL_MODE {
			time.Sleep(config.CONF.WAIT_BEFORE_AUTOMATIC_REQUEST * time.Second)
			log.Printf("[PROPOSER] -> Sending learn request.")
			messageToUser += fmt.Sprintf("Sending learn request.")
			go SendLearn(turnID, proposedV)
		} else {
			log.Printf("[PROPOSER] -> Waiting for user to send learn request; the algorithm suggests: /proposer/send_learn?turn_id=%d&v=%s", turnID, proposedV)
			messageToUser += fmt.Sprintf("Please send a learn request as follows:"+
				" /proposer/send_learn?turn_id=%d&v=%s", turnID, proposedV)
		}

	} else {
		messageToUser = fmt.Sprintf("Quorum has NOT been reached for accept request (%d/%d). ", approvals, len(config.CONF.NODES))
		if highestDecline.Pid != 0 && responseCount >= config.CONF.QUORUM {
			log.Print("[PROPOSER] -> Quorum has NOT been reached for accept request but a majority of nodes is up and running; increment 'seq' and try again.")
			incrementedSeq := highestDecline.Seq + 1
			log.Printf("[COUNTING ACCEPTS] -> highest decline has seq = to %d, incrementing it brings it to %d", highestDecline.Seq, incrementedSeq)
			if !config.CONF.MANUAL_MODE {
				// if we are not in manual mode then wait a random amount of seconds to allow the other proposer(s) to finish
				// rand.float32() generates a number between 0 and 1, adding a flat 0.2 amount
				// r := min + rand.Float64() * (max - min)
				r := rand.Float64() * 5
				time.Sleep(time.Duration(r) * time.Second)
				//time.Sleep(config.CONF.WAIT_BEFORE_AUTOMATIC_REQUEST * time.Second)
				log.Printf("[PROPOSER] -> Sending incremented prepare requests.")
				messageToUser += fmt.Sprintf("Retrying with an incrememented prepare request.")
				go SendPrepare(turnID, incrementedSeq, proposedV)
			} else {
				log.Printf("[PROPOSER] -> Waiting for user to retry the prepare request; the algorithm suggests: /proposer/send_prepare?turn_id=%d&seq=%d&v=%s", turnID, incrementedSeq, proposedV)
				messageToUser += fmt.Sprintf(" Please retry with a higher prepare request as follows:"+
					" /proposer/send_prepare?turn_id=%d&seq=%d&v=%s",
					turnID, incrementedSeq, proposedV)
			}

		} else {
			log.Print("[PROPOSER] -> Quorum has NOT been reached for accept request; the algorithm suggests: do not proceed further, progress is not possible.")
			messageToUser += fmt.Sprintf(" Only %d responded but %d are needed for progress.", responseCount, config.CONF.QUORUM)
		}

	}
	return messageToUser, nil
}

// SendPrepare sends a prepare request to all the acceptors in the network, the values of the prepare request are to be provided by the user (except @v which can remain empty).
func SendPrepare(turnID int, seq int, v string) (messageToUser string) {

	log.Printf("[PROPOSER] -> Starting prepare request; turn_id: %d, seq: %d, v: %s.", turnID, seq, v)
	session := &http.Client{Timeout: time.Second * config.CONF.TIMEOUT}
	ch := make(chan []byte, len(config.CONF.NODES))

	currentV := queries.GetLearntValue(turnID)
	if currentV != "" {
		log.Printf("[PROPOSER] -> Value '%s' has already been learnt for turn_id: %d. Dropping prepare request.", currentV, turnID)
		return fmt.Sprintf("Value for turn_id: %d is already known: %s. Dropping prepare request.", turnID, currentV)
	}

	// send a request for each node
	// responses are saved in ch
	for _, node := range config.CONF.NODES {
		url := node + "/acceptor/receive_prepare"

		// building prepare message
		prepareRequestMessage := messages.GenericMessage{
			TurnID: turnID,            // receiving this from client
			Type:   "prepare_request", // this is just debug info
			Body: messages.Body{
				Message: "sending prepare request", // this is just debug info
				Proposal: proposal.Proposal{
					Pid: config.CONF.PID,
					Seq: seq, // client will pass this param
					V:   v,   // client will pass this param, might be empty string
				},
				Learnt: "",
			},
		}

		go sendPartialRequest(session, url, ch, prepareRequestMessage)
	}

	// counting "promise" responses received in the channel
	messageToUser, err := countAgreements(ch, turnID, seq, v)
	if err != nil {
		log.Printf("Undexpected behavior in SendPrepare: %v", err)
	}

	// if err != nil then agreements is set to 0 inside countAgreements, i dont have to interrupt the flow.
	// agreements may be -1, in that case it means that a learnt value was received in the responses
	return messageToUser
}

// SendAccept sends an accept request to all the acceptors in the network, the values of the accept request are agreed upon during the prepare request, if @v is empty a default value will be assigned to it.
// SendAccept should only be called when it is right to do so, i.e. when the prepare request was "promised" by a majority of nodes.
// Calling this function outside the normal flow of the algorithm does not guarantee the correctness of the system.
// Note that when the node is working in AUTOMATIC mode, this function is called automatically after reaching the quorum for the prepare request.
func SendAccept(turnID int, seq int, v string) (messageToUser string) {

	log.Printf("[PROPOSER] -> Starting accept request; turn_id: %d, seq: %d, v: %s.", turnID, seq, v)
	session := &http.Client{Timeout: time.Second * config.CONF.TIMEOUT}
	ch := make(chan []byte, len(config.CONF.NODES))

	currentV := queries.GetLearntValue(turnID)
	if currentV != "" {
		log.Printf("[PROPOSER] -> Value '%s' has already been learnt for turn_id: %d. Dropping accept request.", currentV, turnID)
		return fmt.Sprintf("Value for turn_id: %d is already known: %s. Dropping prepare request.", turnID, currentV)
	}

	// send a request for each node
	// responses are saved in ch
	for _, node := range config.CONF.NODES {
		url := node + "/acceptor/receive_accept"

		// building accept message
		acceptRequestMessage := messages.GenericMessage{
			TurnID: turnID,
			Type:   "accept_request",
			Body: messages.Body{
				Message: "sending accept request",
				Proposal: proposal.Proposal{
					Pid: config.CONF.PID,
					Seq: seq,
					V:   v,
				},
				Learnt: "",
			},
		}

		go sendPartialRequest(session, url, ch, acceptRequestMessage)
	}

	// counting "accept" responses received in the channel
	messageToUser, err := countApprovals(ch, turnID, seq, v)
	if err != nil {
		log.Printf("Undexpected behavior in SendAccept: %v", err)
	}

	return messageToUser
}

// SendLearn sends an learn request to all the acceptors in the network, the value of the learn request are the values agreed upon during the accept request.
// Note that when the node is working in AUTOMATIC mode, this function is called automatically after reaching the quorum for the accept request.
func SendLearn(turnID int, v string) string {

	log.Printf("[PROPOSER] -> Starting learn request; turn_id: %d, v: %s.", turnID, v)
	session := &http.Client{Timeout: time.Second * config.CONF.TIMEOUT}
	ch := make(chan []byte, len(config.CONF.NODES))

	// send a request for each node
	// responses are saved in ch
	for _, node := range config.CONF.NODES {
		url := node + fmt.Sprintf("/learner/receive_learn")

		// building learn message
		learnRequestMessage := messages.GenericMessage{
			TurnID: turnID,
			Type:   "learn_request",
			Body: messages.Body{
				Message: "sending learn request",
				Proposal: proposal.Proposal{
					Pid: 0,
					Seq: 0,
					V:   v,
				},
				Learnt: "",
			},
		}

		go sendPartialRequest(session, url, ch, learnRequestMessage)
	}

	messageToUser := "Sending learn requests; ignoring responses."

	return messageToUser

}
