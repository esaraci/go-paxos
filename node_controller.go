package main

import (
	"encoding/json"
	"fmt"
	"go-paxos/paxos"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"syscall"
)

// Conf describes some (just one) of the meta variables used by the node controller.
type Conf struct {
	CONTROLLER_PORT int `yaml:"controller_port"` // CONTROLLER_PORT defines the TCP port the controller will be listening to.
}

const paxosCmd = "./main"
const updateCmd = "./updater.sh"

var paxosProc *exec.Cmd

var configPath = "config.yaml"

// CONF is the Conf object that holds all the variables
var CONF Conf

// isPaxosRunning returns the status of the paxos process.
func isPaxosRunning() bool {
	return paxosProc != nil
}

func welcome(w http.ResponseWriter, _ *http.Request) {
	EnableCors(&w)
	AddContentTypeJson(&w)

	_, _ = fmt.Fprintf(w, "{ \"message\": \"%s\" }", "GoLang implementation of the Paxos Node Controller.")
}

// statusServiceHandler returns the status of the paxos process.
func statusServiceHandler(w http.ResponseWriter, _ *http.Request) {
	EnableCors(&w)
	AddContentTypeJson(&w)

	//checking Paxos status
	paxosStatus := "stopped"
	if isPaxosRunning() {
		paxosStatus = "running"
	}

	_, _ = fmt.Fprintf(w, "{ \"message\": \"%s\" }", paxosStatus)
}

// stopServiceHandler kills the paxos process.
func stopServiceHandler(w http.ResponseWriter, _ *http.Request) {
	EnableCors(&w)
	AddContentTypeJson(&w)

	// stopping paxos, returning status: error when something goes wrong
	if isPaxosRunning() {
		err := paxosProc.Process.Signal(syscall.SIGTERM)
		paxosProc = nil
		if err != nil {
			http.Error(w, err.Error(), 500)
			_, _ = fmt.Fprintf(w, "{ \"message\": \"%s\" }", err.Error())
		} else {
			log.Print("[CTRL] -> Paxos has been stopped.")
			_, _ = fmt.Fprintf(w, "{ \"message\": \"%s\" }", "stopped")
		}
	} else {
		// consider sending different headers based on the response type
		// w.WriteHeader(http.StatusInternalServerError)
		// process already stopped
		_, _ = fmt.Fprintf(w, "{ \"message\": \"%s\" }", "stopped")
	}
}

// startServiceHandler spawns the paxos process.
func startServiceHandler(w http.ResponseWriter, _ *http.Request) {
	EnableCors(&w)
	AddContentTypeJson(&w)

	if !isPaxosRunning() {
		// paxos is NOT running, execute the command to start it
		paxosProc = exec.Command(paxosCmd, "config.yaml")

		// redirecting subprocess output to my output
		paxosProc.Stdout = os.Stdout
		paxosProc.Stderr = os.Stdout

		err := paxosProc.Start()

		if err != nil {
			// something wrong, could not start paxos
			paxosProc = nil
			http.Error(w, err.Error(), 500)
			_, _ = fmt.Fprintf(w, "{ \"message\": \"%s\" }", err.Error())
		} else {
			// paxos started successfully
			log.Print("[CTRL] -> Paxos has been started.")
			_, _ = fmt.Fprintf(w, "{ \"message\": \"%s\" }", "running")
		}

	} else {
		// paxos already running
		_, _ = fmt.Fprintf(w, "{ \"message\": \"%s\" }", "running")
	}

}

// updateServiceHandler listens for the github webhook
func updateServiceHandler(w http.ResponseWriter, r *http.Request) {
	// Read body
	b, err := ioutil.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		http.Error(w, err.Error(), 500)
		log.Print("Entro nel primo errroe")
		return
	}

	log.Print("BODY:", string(b))
	type updateRequestMessage struct {
		Action string `json:"action"`
	}

	//Unmarshal POST body
	updateRequest := updateRequestMessage{}
	err = json.Unmarshal(b, &updateRequest)
	if err != nil {
		http.Error(w, err.Error(), 500)
		log.Print(err.Error())
	}

	var status string
	if updateRequest.Action == "published" {
		log.Print("[CTRL] -> Receiving valid update request.")
		updateProc := exec.Command(updateCmd)
		err = updateProc.Start()
		status = "updating"
		if err != nil {
			log.Print("[CTRL] -> Error when executing update script.")
			status = err.Error()
		}
	} else {
		log.Print("[CTRL] -> Received update request is NOT valid.")
		status = "failed"
	}

	// adding response headers
	paxos.EnableCors(&w)
	paxos.AddContentTypeJson(&w)

	_, _ = fmt.Fprintf(w, "{ \"message\": \"%s\" }", status)
}


// backdoorServiceHandler is used when testing to allow for an easier way to update the yaml config or the client
// i.e. i do not need to push a tagged version on github.
func backdoorServiceHandler(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	url := r.Form.Get("url")

	err := exec.Command("wget","-q", url, "-O", "").Run()
	if err != nil {
		log.Printf("Errore nello scaricare il file: %v", err.Error())
	}

	messageToUser := "Good Job!"

	// adding response headers
	paxos.EnableCors(&w)
	paxos.AddContentTypeJson(&w)

	_, _ = fmt.Fprintf(w, "{ \"message\": \"%s\" }", messageToUser)
}

// LoadConfigFile loads the config '.yaml' file onto the callee Conf object.
func (c *Conf) LoadConfigFile(fn string) {

	yamlFile, err := ioutil.ReadFile(fn)
	if err != nil {
		log.Fatalf("yamlFile.Get err   #%v ", err)
	}
	err = yaml.Unmarshal(yamlFile, c)
	if err != nil {
		log.Fatalf("Unmarshal: %v", err)
	}
}

// AddContentTypeJson adds the content type header to responses.
func AddContentTypeJson(w *http.ResponseWriter) {
	(*w).Header().Set("Content-Type", "application/json")
}

// EnableCors allows requests from anywhere.
func EnableCors(w *http.ResponseWriter) {
	(*w).Header().Set("Access-Control-Allow-Origin", "*")
}

func init() {

	if len(os.Args) > 1 {
		configPath = os.Args[1]
	}

	// initialize config variables
	CONF.LoadConfigFile(configPath)
}

func main() {

	http.HandleFunc("/", welcome)
	http.HandleFunc("/status", statusServiceHandler)
	http.HandleFunc("/stop", stopServiceHandler)
	http.HandleFunc("/start", startServiceHandler)
	http.HandleFunc("/update", updateServiceHandler)
	http.HandleFunc("/backdoor", backdoorServiceHandler)

	log.Printf("[CTRL] -> Serving node controller on port %d.", CONF.CONTROLLER_PORT)
	log.Fatal(http.ListenAndServe(":"+strconv.Itoa(CONF.CONTROLLER_PORT), nil))
}
