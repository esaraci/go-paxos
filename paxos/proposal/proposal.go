// Package proposal exposes the Proposal type and some of its methods.
package proposal

// Proposal implements a simple class representing proposals for the paxos algorithm.
// A proposal has a number (n) and a value (v).
// Proposal numbers are supposed to unique and orderable. To achieve so we can represent the number n by using a tuple composed by the sequence number
// and the PID, hence n = (pid, seq). If two proposals have the same sequence number we can use the PID to break ties, since they are unique by definition.
// Two identical proposals can only exist if they refer to different turn ids.
type Proposal struct {
	Pid int    `json:"pid"` // Pid is the (supposedly) unique identifier of the node. 0 is null pid
	Seq int    `json:"seq"` // Seq is the sequence number. 0 is null sequence number
	V   string `json:"v"`   // V is the value being proposed. "" is null value
}

// IsGreaterThan overrides the ">" operator for Proposal objects.
func (p *Proposal) IsGreaterThan(other *Proposal) bool {
	return p.Seq > other.Seq || (p.Seq == other.Seq && p.Pid > other.Pid)
}

// IsLowerThan overrides the "<" operator for Proposal objects.
func (p *Proposal) IsLowerThan(other *Proposal) bool {
	return p.Seq < other.Seq || p.Seq == other.Seq && p.Pid < other.Pid
}

// IsEqualTo overrides the "==" operator for Proposal objects.
func (p *Proposal) IsEqualTo(other *Proposal) bool {
	return p.Seq == other.Seq && p.Pid == other.Pid
}

// IsGEThan overrides the ">=" operator for Proposal objects.
func (p *Proposal) IsGEThan(other *Proposal) bool {
	return p.IsGreaterThan(other) || p.IsEqualTo(other)
}

// IsLEThan overrides the "<=" operator for Proposal object.
func (p *Proposal) IsLEThan(other *Proposal) bool {
	return p.IsLowerThan(other) || p.IsEqualTo(other)
}
