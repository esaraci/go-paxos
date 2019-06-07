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

	MANUAL_MODE  bool          `yaml:"manual_mode"`  // MANUAL_MODE defines whether the nodes proceed automatically through the phases or wait for user interaction.
	TIMEOUT      time.Duration `yaml:"timeout"`      // TIMEOUT defines the time duration (in seconds) waited by the client before assuming a node is not reachable.
	SEEK_TIMEOUT time.Duration `yaml:"seek_timeout"` // SEEK_TIMEOUT defines the time duration (in seconds) needed before performing new a seek request (used only when MANUAL_MODE = false).

	NODES []string `yaml:"nodes"` // NODES defines the list of the paxos nodes of the system.

	QUORUM int `yaml:"quorum"` // QUORUM defines the number of positive responses needed for the algorithm to proceed. It's computed at execution time, but can be provided explicitly.
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
// These are the only fields which can be left blank, each field that is not initialized by this function has to be filled by the user in the .yaml file.
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
