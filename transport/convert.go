package transport

import (
	"github.com/rybo/raft/raft"
	"github.com/rybo/raft/transport/raftpb"
)

// toProto converts a core raft.Message into its wire form.
func toProto(m raft.Message) *raftpb.Message {
	pm := &raftpb.Message{
		Type:       uint32(m.Type),
		To:         m.To,
		From:       m.From,
		Term:       m.Term,
		LogTerm:    m.LogTerm,
		Index:      m.Index,
		Commit:     m.Commit,
		Reject:     m.Reject,
		RejectHint: m.RejectHint,
		Context:    m.Context,
	}
	if len(m.Entries) > 0 {
		pm.Entries = make([]*raftpb.Entry, len(m.Entries))
		for i, e := range m.Entries {
			pm.Entries[i] = entryToProto(e)
		}
	}
	if m.Snapshot != nil {
		pm.Snapshot = snapToProto(*m.Snapshot)
	}
	return pm
}

// fromProto converts a wire message back into a core raft.Message.
func fromProto(pm *raftpb.Message) raft.Message {
	m := raft.Message{
		Type:       raft.MessageType(pm.Type),
		To:         pm.To,
		From:       pm.From,
		Term:       pm.Term,
		LogTerm:    pm.LogTerm,
		Index:      pm.Index,
		Commit:     pm.Commit,
		Reject:     pm.Reject,
		RejectHint: pm.RejectHint,
		Context:    pm.Context,
	}
	if len(pm.Entries) > 0 {
		m.Entries = make([]raft.Entry, len(pm.Entries))
		for i, pe := range pm.Entries {
			m.Entries[i] = entryFromProto(pe)
		}
	}
	if pm.Snapshot != nil {
		s := snapFromProto(pm.Snapshot)
		m.Snapshot = &s
	}
	return m
}

func entryToProto(e raft.Entry) *raftpb.Entry {
	return &raftpb.Entry{
		Term:  e.Term,
		Index: e.Index,
		Type:  uint32(e.Type),
		Data:  e.Data,
	}
}

func entryFromProto(pe *raftpb.Entry) raft.Entry {
	return raft.Entry{
		Term:  pe.Term,
		Index: pe.Index,
		Type:  raft.EntryType(pe.Type),
		Data:  pe.Data,
	}
}

func snapToProto(s raft.Snapshot) *raftpb.Snapshot {
	return &raftpb.Snapshot{
		Data: s.Data,
		Metadata: &raftpb.SnapshotMetadata{
			Index: s.Metadata.Index,
			Term:  s.Metadata.Term,
			ConfState: &raftpb.ConfState{
				Voters:   s.Metadata.ConfState.Voters,
				Learners: s.Metadata.ConfState.Learners,
			},
		},
	}
}

func snapFromProto(ps *raftpb.Snapshot) raft.Snapshot {
	s := raft.Snapshot{Data: ps.Data}
	if md := ps.Metadata; md != nil {
		s.Metadata.Index = md.Index
		s.Metadata.Term = md.Term
		if cs := md.ConfState; cs != nil {
			s.Metadata.ConfState = raft.ConfState{Voters: cs.Voters, Learners: cs.Learners}
		}
	}
	return s
}
