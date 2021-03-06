package main

import (
	"encoding/json"
	"fmt"
	"go-paxos/paxos"
	"go-paxos/paxos/config"
	"go-paxos/paxos/messages"
	"go-paxos/paxos/proposal"
	"go-paxos/paxos/queries"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"time"
)

/*
# ========================================================= #
#                        META HANDLERS                      #
# ========================================================= #
*/

// getProposalHandler handles GET requests on /node/get_proposal.
// This route provides a way to retrieve any proposal's value.
func getProposalHandler(w http.ResponseWriter, r *http.Request) {
	err := r.ParseForm()
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	turnID, _ := strconv.Atoi(r.Form.Get("turn_id"))
	p, _ := queries.GetProposal(turnID)

	// adding response headers
	paxos.EnableCors(&w)
	paxos.AddContentTypeJson(&w)

	// json encoding
	_, _ = fmt.Fprintf(w, paxos.ToJson(p))
}

// getAllProposalsHandler handles GET requests on /node/get_all_proposals.
// This route provides a way to retrieve the list of all the stored proposals.
func getAllProposalsHandler(w http.ResponseWriter, _ *http.Request) {

	m := queries.GetAllProposals()

	// adding response headers
	paxos.EnableCors(&w)
	paxos.AddContentTypeJson(&w)

	// json encoding

	// this is just a workaround to facilitate the response parsing for automatic tests
	_, _ = fmt.Fprint(w, paxos.ToJson(m))
}

// setProposalHandler handles GET requests on /node/set_proposal.
// This route provides a way to insert/update any proposal's value.
func setProposalHandler(w http.ResponseWriter, r *http.Request) {

	err := r.ParseForm()
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	turnID, _ := strconv.Atoi(r.Form.Get("turn_id"))
	pid, _ := strconv.Atoi(r.Form.Get("pid"))
	seq, _ := strconv.Atoi(r.Form.Get("seq"))
	v := r.Form.Get("v")

	p := proposal.Proposal{pid, seq, v}
	err = queries.SetProposal(turnID, p, true) // pretending to be an accept request so the value is forced

	// adding response headers
	paxos.EnableCors(&w)
	paxos.AddContentTypeJson(&w)

	if err != nil {
		http.Error(w, err.Error(), 500)
		_, _ = fmt.Fprintf(w, "{ \"message\": \"%s\"}", err.Error())
	} else {
		_, _ = fmt.Fprintf(w, "{ \"message\": \"%s\"}", "set")
	}
}

// resetProposalHandler handles GET requests on /node/reset_proposal.
// This route provides a way to delete any proposal.
func resetProposalHandler(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	turnID, _ := strconv.Atoi(r.Form.Get("turn_id"))

	err := queries.ResetProposal(turnID)

	// adding response headers
	paxos.EnableCors(&w)
	paxos.AddContentTypeJson(&w)

	if err != nil {
		http.Error(w, err.Error(), 500)
		_, _ = fmt.Fprintf(w, "{ \"message\": \"%s\"}", err.Error())
	} else {
		_, _ = fmt.Fprintf(w, "{ \"message\": \"%s\"}", "reset")
	}
}

// resetProposalHandler handles GET requests on /node/reset_all_proposals.
// This route provides a way to delete all proposals.
func resetAllProposalsHandler(w http.ResponseWriter, _ *http.Request) {
	err := queries.ResetAllProposals()
	// adding response headers
	paxos.EnableCors(&w)
	paxos.AddContentTypeJson(&w)

	// json encoding
	if err != nil {
		http.Error(w, err.Error(), 500)
		_, _ = fmt.Fprintf(w, "{ \"message\": \"%s\"}", err.Error())
	} else {
		_, _ = fmt.Fprintf(w, "{ \"message\": \"%s\"}", "reset")
	}

}

// getLearntValueHandler handles GET requests on /node/get_learnt_value and /learner/get_learnt_value
// This route provides a way to retrieve any learnt value.
func getLearntValueHandler(w http.ResponseWriter, r *http.Request) {

	_ = r.ParseForm()
	turnID, _ := strconv.Atoi(r.Form.Get("turn_id"))

	getLearntRequest := paxos.GetLearntValue(turnID)

	// adding response headers
	paxos.EnableCors(&w)
	paxos.AddContentTypeJson(&w)

	// json encoding
	_, _ = fmt.Fprint(w, paxos.ToJson(getLearntRequest))
}

// getAllLearntValuesHandler handles GET requests on /node/get_all_learnt_values and /learner/get_all_learnt_values.
// This route provides a way to retrieve the list of the stored learnt values.
func getAllLearntValuesHandler(w http.ResponseWriter, _ *http.Request) {
	m := queries.GetAllLearntValues()

	// adding response headers
	paxos.EnableCors(&w)
	paxos.AddContentTypeJson(&w)

	// json encoding
	_, _ = fmt.Fprint(w, paxos.ToJson(m))

}

// setLearntValueHandler handles GET requests on /node/set_learnt_value.
// This route provides a way to insert/update any learnt value.
func setLearntValueHandler(w http.ResponseWriter, r *http.Request) {

	err := r.ParseForm()
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	turnID, _ := strconv.Atoi(r.Form.Get("turn_id"))
	v := r.Form.Get("v")

	err = queries.SetLearntValue(turnID, v) // value set forcefully

	// adding response headers
	paxos.EnableCors(&w)
	paxos.AddContentTypeJson(&w)

	if err != nil {
		http.Error(w, err.Error(), 500)
		_, _ = fmt.Fprintf(w, "{ \"message\": \"%s\"}", err.Error())
	} else {
		_, _ = fmt.Fprintf(w, "{ \"message\": \"%s\"}", "set")
	}
}

// resetLearntValueHandler handles GET requests on /node/reset_learnt_value.
// This route provides a way to delete any learnt value.
func resetLearntValueHandler(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()

	turnID, _ := strconv.Atoi(r.Form.Get("turn_id"))

	err := queries.ResetLearntValue(turnID)

	// adding response headers
	paxos.EnableCors(&w)
	paxos.AddContentTypeJson(&w)

	if err != nil {
		http.Error(w, err.Error(), 500)
		_, _ = fmt.Fprintf(w, "{ \"message\": \"%s\"}", err.Error())
	} else {
		_, _ = fmt.Fprintf(w, "{ \"message\": \"%s\"}", "reset")
	}
}

// resetAllLearntValuesHandler handles GET requests on /node/reset_all_learnt_values.
// This route provides a way to delete all learnt values.
func resetAllLearntValuesHandler(w http.ResponseWriter, r *http.Request) {
	err := queries.ResetAllLearntValues()

	// adding response headers
	paxos.EnableCors(&w)
	paxos.AddContentTypeJson(&w)

	if err != nil {
		http.Error(w, err.Error(), 500)
		_, _ = fmt.Fprintf(w, "{ \"message\": \"%s\"}", err.Error())
	} else {
		_, _ = fmt.Fprintf(w, "{ \"message\": \"%s\"}", "reset")
	}
}

/*
# ========================================================= #
#                     PROPOSER HANDLERS                     #
# ========================================================= #
*/

// sendPrepareHandler handles GET requests on /proposer/send_prepare.
// This route provides a way to trigger the prepare phase.
func sendPrepareHandler(w http.ResponseWriter, r *http.Request) {

	_ = r.ParseForm()

	turnID, _ := strconv.Atoi(r.Form.Get("turn_id"))
	seq, _ := strconv.Atoi(r.Form.Get("seq"))
	v := r.Form.Get("v")

	messageToUser := paxos.SendPrepare(turnID, seq, v, config.CONF.OPTIMIZATION)

	// adding response headers
	paxos.EnableCors(&w)
	paxos.AddContentTypeJson(&w)
	_, _ = fmt.Fprintf(w, "{ \"message\": \"%s\" }", messageToUser)
}

// sendAcceptHandler handles GET requests on /proposer/send_accept.
// This route provides a way to trigger the accept phase.
func sendAcceptHandler(w http.ResponseWriter, r *http.Request) {

	_ = r.ParseForm()

	turnID, _ := strconv.Atoi(r.Form.Get("turn_id"))
	seq, _ := strconv.Atoi(r.Form.Get("seq"))
	v := r.Form.Get("v")
	messageToUser := paxos.SendAccept(turnID, seq, v, config.CONF.OPTIMIZATION)

	// adding response headers
	paxos.EnableCors(&w)
	paxos.AddContentTypeJson(&w)

	_, _ = fmt.Fprintf(w, "{ \"message\": \"%s\" }", messageToUser)
}

// sendLearnHandler handles GET requests on /proposer/send_learn.
// This route provides a way to trigger the learn phase.
func sendLearnHandler(w http.ResponseWriter, r *http.Request) {

	_ = r.ParseForm()

	turnID, _ := strconv.Atoi(r.Form.Get("turn_id"))
	v := r.Form.Get("v")

	messageToUser := paxos.SendLearn(turnID, v)

	// adding response headers
	paxos.EnableCors(&w)
	paxos.AddContentTypeJson(&w)

	_, _ = fmt.Fprintf(w, "{ \"message\": \"%s\" }", messageToUser)

}

/*
# ========================================================= #
#                     ACCEPTOR HANDLERS                     #
# ========================================================= #
*/

// receivePrepareHandler handles POST requests on /acceptor/receive_prepare.
// This route provides a way to handle the prepare phase.
func receivePrepareHandler(w http.ResponseWriter, r *http.Request) {

	// Read body
	b, err := ioutil.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	// Unmarshal POST body
	prepareRequest := messages.GenericMessage{}
	err = json.Unmarshal(b, &prepareRequest)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	prepareResponse := paxos.ReceivePrepare(prepareRequest)

	// adding response headers
	paxos.EnableCors(&w)
	paxos.AddContentTypeJson(&w)

	// json encoding
	_, _ = fmt.Fprint(w, paxos.ToJson(prepareResponse))
}

// receiveAcceptHandler handles POST requests on /acceptor/receive_accept.
// This route provides a way to handle the accept phase.
func receiveAcceptHandler(w http.ResponseWriter, r *http.Request) {

	// Read body
	b, err := ioutil.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	// Unmarshal POST body
	acceptRequest := messages.GenericMessage{}
	err = json.Unmarshal(b, &acceptRequest)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	acceptResponse := paxos.ReceiveAccept(acceptRequest)

	// adding response headers
	paxos.EnableCors(&w)
	paxos.AddContentTypeJson(&w)

	// json encoding
	_, _ = fmt.Fprint(w, paxos.ToJson(acceptResponse))
}

/*
# ========================================================= #
#                     LEARNER HANDLERS                      #
# ========================================================= #
*/

// receiveLearnHandler handles POST requests on /learner/receive_learn.
// This route provides a way to handle the learn phase.
func receiveLearnHandler(w http.ResponseWriter, r *http.Request) {

	// Read body
	b, err := ioutil.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	// Unmarshal body
	learnRequest := messages.GenericMessage{}
	err = json.Unmarshal(b, &learnRequest)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	learnResponse := paxos.ReceiveLearn(learnRequest)

	// adding headers, CORS may be removed
	paxos.EnableCors(&w)
	paxos.AddContentTypeJson(&w)

	// json encoding
	_, _ = fmt.Fprint(w, paxos.ToJson(learnResponse))
}

/*
# ========================================================= #
#                      SEEKER HANDLERS                      #
# ========================================================= #
*/

// sendSeekHandler handles GET requests on /seeker/send_seek.
// This route provides a way to trigger a seek request.
func sendSeekHandler(w http.ResponseWriter, _ *http.Request) {
	paxos.SendSeek()

	// adding response headers
	paxos.EnableCors(&w)
	paxos.AddContentTypeJson(&w)
	_, _ = fmt.Fprintf(w, "{ \"message\": \"%s\"}", "ok")
}

// receiveSeekHandler handles GET requests on /seeker/receive_seek.
// This route provides a way to handle seek requests.
func receiveSeekHandler(w http.ResponseWriter, r *http.Request) {

	// Read body
	b, err := ioutil.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	// Unmarshal body
	seekRequest := messages.NewValuesRequest{}
	err = json.Unmarshal(b, &seekRequest)

	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	seekResponse := paxos.ComputeNewValuesResponse(seekRequest)

	// adding response headers
	paxos.EnableCors(&w)
	paxos.AddContentTypeJson(&w)

	// json encoding
	_, _ = fmt.Fprint(w, paxos.ToJson(seekResponse))

}

/*
# ========================================================= #
#                       OTHER HANDLERS                      #
# ========================================================= #
*/

// welcomeHandler is the handler of GET requests to the root route "/" or to any other non existing route.
func welcomeHandler(w http.ResponseWriter, _ *http.Request) {

	// adding response headers
	paxos.EnableCors(&w)
	paxos.AddContentTypeJson(&w)

	_, _ = fmt.Fprintf(w, "{ \"message\": \"%s\" }", "GoLang implementation of the Paxos Algorithm.")
}

// info handles GET requests to route /info and returns a string containing the execution mode, the PID of the node, and the language this client is written in.
func infoHandler(w http.ResponseWriter, _ *http.Request) {
	language := "golang"
	var mode string

	if config.CONF.MANUAL_MODE {
		mode = "manual"
	} else {
		mode = "automatic"
	}

	// adding response headers
	paxos.EnableCors(&w)
	paxos.AddContentTypeJson(&w)

	// json encoding
	_, _ = fmt.Fprintf(w, "{ \"message\": \"%s@%s@%d\" }", language, mode, config.CONF.PID)
}

func startSeekingForeverHandler(w http.ResponseWriter, _ *http.Request) {
	go seek4ever()

	// adding response headers
	paxos.EnableCors(&w)
	paxos.AddContentTypeJson(&w)

	// json encoding
	_, _ = fmt.Fprint(w, "{ \"message\": \"ok\" }")
}

// seek4ever triggers a seek request every x seconds. The amount of seconds can be changed in the '.yaml' file.
// this function is only called when in AUTOMATIC mode.
func seek4ever() {
	for {
		time.Sleep(config.CONF.SEEK_TIMEOUT * time.Second)
		r := rand.Float64()
		log.Print("[SEEKER] -> Tossing a coin...")
		if r < 0.75 {
			log.Print("[SEEKER] -> Heads! Calling for seek()")
			paxos.SendSeek()
		} else {
			log.Printf("[SEEKER] -> Tails! Seeking procedure will be skipped")
		}

	}

}

func init() {

	rand.Seed(time.Now().UTC().UnixNano())
	configPath := "./config.yaml"

	// config path can be specified as an argument from command line
	if len(os.Args) > 1 {
		configPath = os.Args[1]
	}

	// initialize config variables
	config.CONF.LoadConfigFile(configPath)
	config.CONF.FillEmptyFields()

	// checking if database exists
	info, err := os.Stat(config.CONF.DB_PATH)
	if os.IsNotExist(err) {
		// database does not exist, create the database and initialize it
		_, err = os.Create(config.CONF.DB_PATH)
		if err != nil {
			log.Print("[ERROR] -> Could not create database. Something is wrong.")
		} else {
			// now i certainly have a db file.
			queries.SQLitePrepareDBConn()
			queries.InitDatabase()
		}

		// might have other err cases i want to handle
	} else if info.IsDir() {
		// file does exist but it's a folder, exit and ask the user to change the filename.
		log.Fatalf("[ERROR] -> %s is a folder. The database has NOT been created. Change filename and retry.", config.CONF.DB_PATH)
	}

	queries.PrepareDBConn()
}

func main() {

	// META ROUTES
	http.HandleFunc("/", welcomeHandler)
	http.HandleFunc("/info", infoHandler)

	// proposal values handling
	http.HandleFunc("/node/get_proposal", getProposalHandler)
	http.HandleFunc("/node/get_all_proposals", getAllProposalsHandler)
	http.HandleFunc("/node/set_proposal", setProposalHandler)
	http.HandleFunc("/node/reset_proposal", resetProposalHandler)
	http.HandleFunc("/node/reset_all_proposals", resetAllProposalsHandler)

	// learnt value handling
	http.HandleFunc("/node/get_learnt_value", getLearntValueHandler)
	http.HandleFunc("/node/get_all_learnt_values", getAllLearntValuesHandler)
	http.HandleFunc("/node/set_learnt_value", setLearntValueHandler) // same as receiveLearnHandler but it's a GET request
	http.HandleFunc("/node/reset_learnt_value", resetLearntValueHandler)
	http.HandleFunc("/node/reset_all_learnt_values", resetAllLearntValuesHandler)

	// PROPOSER ROUTES
	http.HandleFunc("/proposer/send_prepare", sendPrepareHandler)
	http.HandleFunc("/proposer/send_accept", sendAcceptHandler)
	http.HandleFunc("/proposer/send_learn", sendLearnHandler)

	// SEEKER ROUTES
	http.HandleFunc("/seeker/send_seek", sendSeekHandler)       // --> calls send seek manually
	http.HandleFunc("/seeker/receive_seek", receiveSeekHandler) // --> calls send seek manually

	http.HandleFunc("/seeker/start_seeking_forever", startSeekingForeverHandler)

	// ACCEPTOR ROUTES
	http.HandleFunc("/acceptor/receive_prepare", receivePrepareHandler)
	http.HandleFunc("/acceptor/receive_accept", receiveAcceptHandler)

	// LEARNER ROUTES
	http.HandleFunc("/learner/receive_learn", receiveLearnHandler)
	http.HandleFunc("/learner/get_learnt_value", getLearntValueHandler)          // --> redundant, clone of /learner/get_learnt_value
	http.HandleFunc("/learner/get_all_learnt_values", getAllLearntValuesHandler) // --> redundant, clone of /learner/get_all_learnt_values

	if !config.CONF.MANUAL_MODE {
		log.Printf("[MAIN] -> Automatic Mode is activated for this node. Timeouts: Prepare -(%ds)-> Accept -(%ds)-> Learn.", config.CONF.WAIT_BEFORE_AUTOMATIC_REQUEST, config.CONF.WAIT_BEFORE_AUTOMATIC_REQUEST)
		if config.CONF.SEEK_ACTIVE {
			log.Printf("[MAIN] -> Seeking is ACTIVATED and it will be performed every %d seconds", config.CONF.SEEK_TIMEOUT)
			go seek4ever()
		} else {
			log.Printf("[MAIN] -> Seeking is DEACTIVATED.")
		}
	}

	log.Printf("[MAIN] -> Serving paxos on port %d.", config.CONF.PORT)
	log.Fatal(http.ListenAndServe("0.0.0.0:"+strconv.Itoa(config.CONF.PORT), nil))

}
