// Package messages exposes some of the structures used for internal communication.
// These structure are usually marshalled (to json) before being sent to remote nodes.
// Messages received from remote nodes are usually unmarshalled (from json) to these structures.
package messages

import (
	"go-paxos/paxos/proposal"
)

// Body is the body of the message being sent. It contains the main contents of the message and is the "wrappee" of the more general structure called GenericMessage.
type Body struct {
	Message  string            `json:"message"`  // Message is an arbitrary string, in some messages is just used for debugging purposes, in other it is crucial.
	Proposal proposal.Proposal `json:"proposal"` // Proposal is a Proposal instance.
	Learnt   string            `json:"learnt"`   // Learnt is a field used to notify the receiver that a value has already been learnt for the current turn id. The value of the field is the value itself. If "" is found then no value has been learnt for this turn ID.
}

// GenericMessage is used as wrapper for the Body type. It adds two crucial fields: the TurnID field and the Type field.
type GenericMessage struct {
	TurnID int    `json:"turn_id"`      // TurnID is an integer that uniquely identifies which turn the messages refer to.
	Type   string `json:"message_type"` // Type is
	Body   Body   `json:"message_body"`
}

// ProposalWithTid is self explaining. It appends a turn id field to the proposal.
// This class is useful when printing all or multiple proposals values.
type ProposalWithTid struct {
	TurnID   int               `json:"turn_id"`
	Proposal proposal.Proposal `json:"proposal"`
}

// LearntWithTid is the representation of an entry of the 'learnt' table.
// it's composed of a turn id field and a learnt field.
// This class is useful when printing all or multiple learnt values.
type LearntWithTid struct {
	TurnID int    `json:"turn_id"` // TurnID is an integer that uniquely identifies which turn the messages refer to.
	Learnt string `json:"learnt"`  // Learnt is a string representing the learnt value for this turn id. If "" is found then no value has been learnt for this turn id.
}

// NewValuesRequest describes what our highest learnt turn id is and which past turn ids we are missing.
type NewValuesRequest struct {
	Missing []int `json:"missing"` // Missing is a list of missing turn ids. See ComputeNewValuesRequest in 'seeker.go' to understand how this list is computed.
	Last    int   `json:"last"`    // Last is the highest turn id with a learnt value known to us.
}

// NewValuesResponse describes the response received after a NewValueRequest message was sent. When receiving this kind of message all of its contents must be learned.
type NewValuesResponse struct {
	// map having integers as keys and strings as values; the keys are the turn ids, the values are the learnt values for the respective key.
	// See ComputeNewValueResponse in'seeker.go' to understand hoe this map is computed.
	ToLearn map[int]string `json:"to_learn"`
}
