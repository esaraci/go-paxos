/*

An acceptor can receive two kinds of requests from proposers:
prepare requests and accept requests.
An acceptor can ignore any request without compromising safety.
So, we need to say only when it is allowed to respond to a request.
(1) It can always respond to a prepare request.
(2) It can respond to an accept request, accepting the messages, IFF it has not promised not to do so.
In other words: an acceptor can accept a messages request (accept_request(n,v)) numbered n IFF it has not
responded to a prepare request having a number greater than n

Note that if an acceptor receives a prepare_request(n),
but it has already responded to a prepare_request(x>n), there is no point in responding
to such prepare_request since the accept_request(n) will be ignored (it has already promised
not to accept proposals with values <x). So, the acceptor could ignore:
    (1) prepare_request(n) with n<x, x being a prepare_request accepted before
    (2) prepare_request(n) when it has already accepted the messages with value n before.
    BTW: the acceptor in (1) and (2) should still respond to the proposer
        by telling him that there's a messages with a higher number on the network
        and that it should drop its messages. (or incremenet its n)


With this optimization, an acceptor needs to remember only the highest-
numbered messages that it has ever accepted and the number of the highest-
numbered prepare request to which it has responded. Because P2 c must
be kept invariant regardless of failures, an acceptor must remember this
information even if it fails and then restarts.

*/

// Package paxos implements the main components of the Paxos distributed consensus algorithm.
package paxos

import (
	"go-paxos/paxos/messages"
	"go-paxos/paxos/proposal"
	"go-paxos/paxos/queries"
	"log"
)

// ReceivePrepare implements the acceptor's behaviour when receiving a prepare request.
// This function compares the stored proposal (@oldP) against the
// proposal received as input (@newP) and returns a "promise" when
// @newP is STRICTLY higher than @oldP, otherwise it returns a "retry" message.
// In both cases @oldP (which might be null) is appended to the final response message.
// In some cases the acceptor might notice that a value has already been learnt
// for the requested proposal, in that case this information will also be appended to the response message.
// This function is very similar to ReceiveAccept, the distinction is made just to avoid multiple small if-else clauses.
func ReceivePrepare(prepareRequest messages.GenericMessage) messages.GenericMessage {
	// extracting info from message
	turnID := prepareRequest.TurnID
	pid := prepareRequest.Body.Proposal.Pid
	seq := prepareRequest.Body.Proposal.Seq
	proposedV := prepareRequest.Body.Proposal.V

	log.Printf("[ACCEPTOR] -> Receiving prepare request with turn id: %d, pid: %d, seq: %d, v: %s.", turnID, pid, seq, proposedV)

	// checking if i already have a learned value for this turn_id
	currentV := queries.GetLearntValue(turnID)
	if currentV != "" {
		// WARNING, WE ALREADY HAVE A LEARNT VALUE FOR THIS TURN_ID
		// DO NOT PROCEED FURTHER AND LET PROPOSER KNOW
		// CAREFUL, THIS IS AN ALTERNATIVE (function) SINK
		earlyResult := messages.GenericMessage{
			TurnID: turnID,
			Type:   "accept_response",
			Body: messages.Body{
				Message:  "already learnt",
				Proposal: proposal.Proposal{},
				Learnt:   currentV,
			},
		}
		log.Printf("[ACCEPTOR] -> Value '%s' has already been learnt for turn id %d. Let the proposer know immediately and drop any further computation.", currentV, turnID)
		return earlyResult
	}

	// we DO NOT currently have a learnt value for turn_id
	// @ok is a boolean variable, true iff @oldP is valid (i.e. @oldP.pid && @oldP.seq != nil)
	oldP, ok := queries.GetProposal(turnID)
	newP := proposal.Proposal{Pid: pid, Seq: seq, V: proposedV}

	// computing @response
	// @response is a status variable, it holds the result of the message we will be sending back.
	// Default is "retry" since an acceptor can always ```safely "ignore" a proposal request.```
	response := "retry"
	if !ok || newP.IsGreaterThan(&oldP) {

		err := queries.SetProposal(turnID, newP, false)
		if err != nil {
			// could not store @newP
			log.Print("[ACCEPTOR] -> Refusing prepare request, could not store the new proposal. Here's the error: ", err.Error())
		} else {
			// no errors while storing @newP, return a promise
			response = "promise"
			log.Printf("[ACCEPTOR] -> Seq: %d pid: %d is the highest proposal for turn id %d; sending back a promise.", seq, pid, turnID)
		}
	} else {
		// @oldP is higher than @newP
		response = "retry"
		log.Printf("[ACCEPTOR] -> Seq: %d, pid: %d is not strictly higher than the current highest proposal (seq: %d, pid: %d) for turn id %d; sending back a retry.", seq, pid, oldP.Seq, oldP.Pid, turnID)
	}
	// @response is now set

	// building response message
	result := messages.GenericMessage{
		TurnID: turnID,            // turn_id
		Type:   "accept_response", // just debug info
		Body: messages.Body{
			Message:  response, // either "retry" or "promise"
			Proposal: oldP,     // passing oldP as requested by the protocol
			Learnt:   currentV, // passing current_v, either empty or actual learned value
		},
	}

	return result

}

// ReceiveAccept implements the acceptor's behaviour when receiving an accept request.
// This function compares the stored proposal (@oldP) against the
// proposal received as input (@newP) and returns an "accept" when
// @newP is strictly higher or equal to @oldP, otherwise it returns a "decline" message.
// In both cases @oldP (which might be null) is appended to the final response message.
// In some cases the acceptor might notice that a value has already been learnt
// for the requested proposal, in that case this information will also be appended to the response message.
// This function is very similar to ReceivePrepare, the distinction is made just to avoid multiple small if-else clauses.
func ReceiveAccept(acceptRequest messages.GenericMessage) messages.GenericMessage {

	// extracting info from message
	turnID := acceptRequest.TurnID
	pid := acceptRequest.Body.Proposal.Pid
	seq := acceptRequest.Body.Proposal.Seq
	v := acceptRequest.Body.Proposal.V

	log.Printf("[ACCEPTOR] -> Receiving accept request with turn id: %d, pid: %d, seq: %d, v: %s.", turnID, pid, seq, v)

	currentV := queries.GetLearntValue(turnID)
	if currentV != "" {
		// WARNING, WE ALREADY HAVE A LEARNT VALUE FOR THIS TURN_ID
		// DO NOT PROCEED FURTHER AND LET PROPOSER KNOW
		// CAREFUL, THIS IS AN ALTERNATIVE SINK
		earlyResult := messages.GenericMessage{
			TurnID: turnID,
			Type:   "accept_response",
			Body: messages.Body{
				Message:  "already learnt",
				Proposal: proposal.Proposal{},
				Learnt:   currentV,
			},
		}
		log.Printf("[ACCEPTOR] -> Value '%s' has already been learnt for turn id %d. Let the proposer know immediately and drop any further computation.", currentV, turnID)
		return earlyResult
	}

	// we DO NOT currently have a learnt value for turn_id
	// @ok is a boolean variable, true iff @oldP is valid (i.e. @oldP.pid, @oldP.seq != NULL)
	oldP, ok := queries.GetProposal(turnID)
	newP := proposal.Proposal{Pid: pid, Seq: seq, V: v}

	// response is a status var that holds the response message we're sending back
	response := "decline"
	if !ok || newP.IsGEThan(&oldP) {
		// if (oldP is NOT valid) OR (oldP is valid but newP>=oldP)
		// oldP is probably newP saved during the prepare request.

		// CLARIFICATION --> this has to be >= (not only > as in the prepare request)
		// so that when proposal numbered n is promised after a prepare_request,
		// the following accept_request with same number n wont be declined

		// save newP
		err := queries.SetProposal(turnID, newP, true)
		if err != nil {
			// could not store @newP
			log.Print("[ACCEPTOR] -> Declining accept request, could not store the new proposal. Here's the error: ", err.Error())
		} else {
			// no errors storing @newP, return a promise
			response = "accept"
			log.Printf("[ACCEPTOR] -> Seq: %d pid: %d is the highest proposal for turn id %d; sending back an accept.", seq, pid, turnID)
		}
	} else {
		// @oldP is valid and higher than @newP
		response = "decline"
		log.Printf("[ACCEPTOR] -> Seq: %d, pid: %d is not higher than (or equal to) the current highest proposal (seq: %d, pid: %d) for turn id %d; sending back a decline.", seq, pid, oldP.Seq, oldP.Pid, turnID)
	}

	// composing message
	result := messages.GenericMessage{
		TurnID: turnID, // turn_id
		Type:   "accept_response",
		Body: messages.Body{
			Message:  response, // either "accept" or "decline"
			Proposal: oldP,     // sending oldP as suggested by the protocol
			Learnt:   currentV, // either "" or an actual learned value
		},
	}

	return result
}
