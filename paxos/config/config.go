// Package config exposes some static variables loaded through a .yaml file used throughout the Paxos algorithm.
package config

import (
	"fmt"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"math/rand"
	"time"
)

// CONF is the Conf object which holds all the variables
var CONF Conf

// Conf is a type describing some of the meta variables used by different parts of the algorithm.
type Conf struct {
	DB_PATH   string `yaml:"db_path"`   // DB_PATH locates the database file.
	PID       int    `yaml:"pid"`       // PID is the identifier of the node, PID is supposed to unique.
	V_DEFAULT string `yaml:"v_default"` // V_DEFAULT defines what is the default value proposed by the node for the accept request when no prior value has been proposed.

	PORT int `yaml:"port"` // PORT defines the TCP port to which the web server will be listening.

	MANUAL_MODE                   bool          `yaml:"manual_mode"`                   // MANUAL_MODE defines whether the nodes proceed automatically through the phases or wait for user interaction.
	TIMEOUT                       time.Duration `yaml:"timeout"`                       // TIMEOUT defines the time duration (in seconds) waited by the client before assuming a node is not reachable.
	SEEK_ACTIVE                   bool          `yaml:"seek_active"`                   // SEEK_ACTIVE defines whether seeking activities are active. When MANUAL_MODE is true, SEEK_ACTIVE will be ignored.
	SEEK_TIMEOUT                  time.Duration `yaml:"seek_timeout"`                  // SEEK_TIMEOUT defines the time duration (in seconds) needed before performing new a seek request (used only when MANUAL_MODE = false).
	WAIT_BEFORE_AUTOMATIC_REQUEST time.Duration `yaml:"wait_before_automatic_request"` // WAIT_BEFORE_AUTOMATIC_REQUEST defines the time duration (in seconds) waited by the proposer before sending and accept request of a learn_request when in AUTOMATIC_MODE

	PR_PROPOSALS float64 `yaml:"pr_proposals"` // PR_PROPOSALS defines the probability of removing a proposal from the dangling proposals list. It's used by the seeker to reduce the amount of requests
	PR_NODES     float64 `yaml:"pr_nodes"`     // PR_NODES defines the probability to choose a node towards which to perform a seek request

	NODES  []string `yaml:"nodes"`  // NODES defines the list of the paxos nodes of the system.
	QUORUM int      `yaml:"quorum"` // QUORUM defines the number of positive responses needed for the algorithm to proceed. It's computed at execution time, but can be provided explicitly.

	NUMBER_OF_TIDS int    `yaml:"number_of_tids"`
	LISTENER_IP    string `yaml:"listener_ip"`

	DB_TYPE    string `yaml:"db_type"`

	OPTIMIZATION 	bool `yaml:"optimization"`
}

// LoadConfigFile loads the config '.yaml' file onto the callee Conf object.
func (c *Conf) LoadConfigFile(fn string) {

	yamlFile, err := ioutil.ReadFile(fn)
	if err != nil {
		log.Fatalf("yamlFile.Get err %v ", err)
	}
	err = yaml.Unmarshal(yamlFile, c)
	if err != nil {
		log.Fatalf("Unmarshal: %v", err)
	}
}

// FillEmptyFields fills in those fields that were left empty in the .yaml file or those which need a run-time computation.
// These are the only fields which can be left blank, if one of the field is not initialized by this function, has to be initialized by the user in the '.yaml' file.
func (c *Conf) FillEmptyFields() {

	if c.PID == 0 {
		c.PID = rand.Intn(10000)
	}

	if c.V_DEFAULT == "" {
		c.V_DEFAULT = fmt.Sprintf("paxos@%d", c.PID)
	}

	if c.TIMEOUT == 0 {
		c.TIMEOUT = 2
	}

	if c.SEEK_TIMEOUT == 0 {
		c.SEEK_TIMEOUT = 5
	}

	if c.QUORUM == 0 {
		c.QUORUM = len(c.NODES)/2 + 1
	}

}
