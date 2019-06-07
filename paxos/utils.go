package paxos

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go-paxos/paxos/config"
	"go-paxos/paxos/messages"
	"go-paxos/paxos/proposal"
	"io/ioutil"
	"log"
	"net/http"
	"time"
)

// sendPartialRequest sends HTTP POST requests and saves the responses into a channel. If the target is not reachable a nil response is added to the channel.
func sendPartialRequest(session *http.Client, url string, resBuffer chan []byte, message interface{}) {

	// sending post requests
	jsonContents, err := json.MarshalIndent(message, "", "	")
	if err != nil {
		fmt.Print(err.Error())
	}
	res, err := session.Post(url, "application/json", bytes.NewBuffer(jsonContents))

	if res != nil {
		// i need this because if res == nil
		// i would not be able to call res.Body.Close()
		defer res.Body.Close()
	}
	if err != nil {
		log.Printf("[UTILS] -> Node %s is not reachable, adding null response to channel.", url)
		resBuffer <- nil

	} else {
		//log.Printf("[UTILS] -> Node %s answered, adding response to channel.", url)
		body, _ := ioutil.ReadAll(res.Body)
		resBuffer <- body

	}

}

// floodLearntValue floods the network with learnt requests for the new learnt value.
// This is very similar to the SendLearn function of the proposer.
// Sending learn requests is usually the proposer's job; in this case is the learner that has to do it, but in order to prevent the learner from knowing anything about the proposer
// I cannot call SendLearn since that function assumes the existence of a Proposer component.
// In other words, im repeating some code to preserve separation between components.
func floodLearntValue(turnID int, v string) {
	session := &http.Client{Timeout: time.Second * config.CONF.TIMEOUT}
	learnRequest := messages.GenericMessage{
		TurnID: turnID,
		Type:   "learn_flood",
		Body: messages.Body{
			Message:  "",
			Proposal: proposal.Proposal{V: v}, // sending learnt value
			Learnt:   "",                      // this is only used in responses, not requests
		},
	}

	ch := make(chan []byte, len(config.CONF.NODES))
	for _, node := range config.CONF.NODES {
		url := node + "/learner/receive_learn"
		go sendPartialRequest(session, url, ch, learnRequest)
	}

}

// ResponseHasLearntValue checks whether the response contains a non empty string in the 'learnt' field.
// If that is true it means that the responding node has already learnt a value for the current turn id.
func ResponseHasLearntValue(responseMessage messages.GenericMessage) bool {
	// return true only if 'learnt' is not an empty string
	return responseMessage.Body.Learnt != ""
}

// This function is never used since no error handling is done this way.
// ResponseHasErrors checks whether the response message has errors.
//func ResponseHasErrors(responseMessage messages.GenericMessage) bool {
//	return responseMessage.Type == "error"
//}

// ToJson is used to marshal interfaces into a valid json string.
func ToJson(i interface{}) string {
	res, _ := json.MarshalIndent(i, "", "	")
	return string(res)
}

// AddContentTypeJson adds the content type header to responses.
func AddContentTypeJson(w *http.ResponseWriter) {
	(*w).Header().Set("Content-Type", "application/json")
}

// EnableCors allows requests from anywhere.
func EnableCors(w *http.ResponseWriter) {
	(*w).Header().Set("Access-Control-Allow-Origin", "*")
}
