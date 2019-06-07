package paxos

import (
	"go-paxos/paxos/messages"
	"go-paxos/paxos/proposal"
	"go-paxos/paxos/queries"
	"log"
)

// GetLearntValue returns a message with the 'learnt' field containing the value (@v) of the proposal with turn ID = @turnID.
// If the requested turn ID does not exist, the 'learnt' field will contain an empty string.
func GetLearntValue(turnID int) messages.GenericMessage {
	v := queries.GetLearntValue(turnID)

	getLearntResponse := messages.GenericMessage{
		TurnID: turnID,
		Type:   "get_learnt_response",
		Body: messages.Body{
			Message:  "Value is in 'learnt' field; if empty consider it as NULL.",
			Proposal: proposal.Proposal{},
			Learnt:   v,
		},
	}

	return getLearntResponse
}

// ReceiveLearn implements the learner's behaviour when receiving a learn request.
// If the proposed value has not been learnt yet it gets learnt immediately and learn requests with that value are sent to each known node.
// If the proposed value has already been learnt then no action is performed.
// If we get a proposal to learn a value which is different from the value we already have for that turn id
func ReceiveLearn(learnRequest messages.GenericMessage) messages.GenericMessage {

	turnID := learnRequest.TurnID
	proposedV := learnRequest.Body.Proposal.V  // value we are requested to learn
	currentV := queries.GetLearntValue(turnID) // value we have already learnt for this @turnID, might be "" of course

	log.Printf("[LEARNER] -> Receiving learn request with turn id: %d, v: %s.", turnID, proposedV)

	learnResponse := messages.GenericMessage{
		TurnID: turnID,
		Type:   "learn_response",
		Body: messages.Body{
			Message:  "",
			Proposal: proposal.Proposal{},
			Learnt:   "",
		},
	}

	if proposedV != currentV && currentV != "" {
		log.Print("[LEARNER] -> Refusing learn request. I already have a learnt value for this turn id, please respect the algorithm.")
		learnResponse.Body.Message = "Trying to learn a different value, please respect the algorithm."
	} else {

		err := queries.SetLearntValue(turnID, proposedV)

		if err != nil {
			log.Print("[LEARNER] -> Refusing learn request, could not store the new proposal. Here's the error: ", err.Error())
			learnResponse.Body.Message = "Fail: " + err.Error()
		} else {

			if currentV == proposedV {
				log.Printf("[LEARNER] -> Value '%s' has already been learnt for turn id %d. Don't need to learn that again.", proposedV, turnID)
			} else {
				// currentV was empty, flood the other nodes with requests.
				// PropagateLearnedValue(turn_id, v)
				log.Printf("[LEARNER] -> Learning and propagating '%s' for turn_id: %d.", proposedV, turnID)
				learnResponse.Body.Message = "value stored"
				learnResponse.Body.Learnt = proposedV

				go floodLearntValue(turnID, proposedV)

			}
		}

	}

	return learnResponse
}
