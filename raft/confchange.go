package raft

import "encoding/binary"

// encodeConfChange serializes a ConfChange into an EntryConfChange payload.
func encodeConfChange(cc ConfChange) []byte {
	b := make([]byte, 9)
	b[0] = byte(cc.Type)
	binary.BigEndian.PutUint64(b[1:], cc.NodeID)
	return b
}

// DecodeConfChange parses an EntryConfChange payload. The driver calls this when
// it encounters an EntryConfChange in CommittedEntries, then passes the result
// to RawNode.ApplyConfChange.
func DecodeConfChange(data []byte) ConfChange {
	if len(data) < 9 {
		return ConfChange{}
	}
	return ConfChange{
		Type:   ConfChangeType(data[0]),
		NodeID: binary.BigEndian.Uint64(data[1:]),
	}
}
