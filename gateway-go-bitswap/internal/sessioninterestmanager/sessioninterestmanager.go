package sessioninterestmanager

import (
	"fmt"
	"sync"

	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	)

// SessionInterestManager records the CIDs that each session is interested in.
type SessionInterestManager struct {
	lk    sync.RWMutex
	wants map[cid.Cid]map[uint64]bool
	cwants map[cid.Cid]map[uint64]int
	
}

// New initializes a new SessionInterestManager.
func New() *SessionInterestManager {
	return &SessionInterestManager{
		// Map of cids -> sessions -> bool
		//
		// The boolean indicates whether the session still wants the block
		// or is just interested in receiving messages about it.
		//
		// Note that once the block is received the session no longer wants
		// the block, but still wants to receive messages from peers who have
		// the block as they may have other blocks the session is interested in.
		wants: make(map[cid.Cid]map[uint64]bool),
		cwants: make(map[cid.Cid]map[uint64]int),

	}
}

// When the client asks the session for blocks, the session calls
// RecordSessionInterest() with those cids.
func (sim *SessionInterestManager) RecordSessionInterest(ses uint64, ks []cid.Cid) {
	sim.lk.Lock()
	defer sim.lk.Unlock()

	// For each key
	for _, c := range ks {
		// Record that the session wants the blocks
		if want, ok := sim.wants[c]; ok {
			want[ses] = true
		} else {
			sim.wants[c] = map[uint64]bool{ses: true}
		}
	}
}

func (sim *SessionInterestManager) RecordSessionInterestC(ses uint64, k cid.Cid, count int) {
	sim.lk.Lock()
	defer sim.lk.Unlock()

	if want, ok := sim.cwants[k]; ok {
		if _, ok := want[ses]; ok{
			want[ses] += count
		}
	} else {
		sim.cwants[k] = map[uint64]int{ses: count}
	}
}


	// When the session shuts down it calls RemoveSessionInterest().
// Returns the keys that no session is interested in any more.
func (sim *SessionInterestManager) RemoveSession(ses uint64) []cid.Cid {
	sim.lk.Lock()
	defer sim.lk.Unlock()
	fmt.Println("Debug: sim-removesession")

	// The keys that no session is interested in
	deletedKs := make([]cid.Cid, 0)

	// For each known key
	for c := range sim.wants {
		// Remove the session from the list of sessions that want the key
		delete(sim.wants[c], ses)

		// If there are no more sessions that want the key
		if len(sim.wants[c]) == 0 {
			// Clean up the list memory
			delete(sim.wants, c)
			// Add the key to the list of keys that no session is interested in
			deletedKs = append(deletedKs, c)
		}
	}

	sim.decodingkLk.Lock()
	defer sim.decodingkLk.Unlock()
	for _, k := range deletedKs{
		fmt.Println("Debug: sim-removedecoder", k)
		delete(sim.decoders, k)
		delete(sim.decoderdata, k)
	}

	return deletedKs
}

// When the session receives blocks, it calls RemoveSessionWants().
func (sim *SessionInterestManager) RemoveSessionWants(ses uint64, ks []cid.Cid) {
	sim.lk.Lock()
	defer sim.lk.Unlock()

	// For each key
	for _, c := range ks {
		// If the session wanted the block
		if wanted, ok := sim.wants[c][ses]; ok && wanted {
			// Mark the block as unwanted
			sim.wants[c][ses] = false
		}
	}
}

// When a request is cancelled, the session calls RemoveSessionInterested().
// Returns the keys that no session is interested in any more.
func (sim *SessionInterestManager) RemoveSessionInterested(ses uint64, ks []cid.Cid) []cid.Cid {
	sim.lk.Lock()
	defer sim.lk.Unlock()

	// The keys that no session is interested in
	deletedKs := make([]cid.Cid, 0, len(ks))

	// For each key
	for _, c := range ks {
		// If there is a list of sessions that want the key
		if _, ok := sim.wants[c]; ok {
			// Remove the session from the list of sessions that want the key
			delete(sim.wants[c], ses)

			// If there are no more sessions that want the key
			if len(sim.wants[c]) == 0 {
				// Clean up the list memory
				delete(sim.wants, c)
				// Add the key to the list of keys that no session is interested in
				deletedKs = append(deletedKs, c)
			}
		}
	}

	sim.decodingkLk.Lock()
	defer sim.decodingkLk.Unlock()
	for _,k:=range deletedKs{
		delete(sim.decoders, k)
		delete(sim.decoderdata, k)
	}

	return deletedKs
}

// The session calls FilterSessionInterested() to filter the sets of keys for
// those that the session is interested in
func (sim *SessionInterestManager) FilterSessionInterested(ses uint64, ksets ...[]cid.Cid) [][]cid.Cid {
	sim.lk.RLock()
	defer sim.lk.RUnlock()

	// For each set of keys
	kres := make([][]cid.Cid, len(ksets))
	for i, ks := range ksets {
		// The set of keys that at least one session is interested in
		has := make([]cid.Cid, 0, len(ks))

		// For each key in the list
		for _, c := range ks {
			// If there is a session that's interested, add the key to the set
			if _, ok := sim.wants[c][ses]; ok {
				has = append(has, c)
			}
		}
		kres[i] = has
	}
	return kres
}

// When bitswap receives blocks it calls SplitWantedUnwanted() to discard
// unwanted blocks
func (sim *SessionInterestManager) SplitWantedUnwanted(blks []blocks.Block) ([]blocks.Block, []blocks.Block, []*blocks.CodedBlock) {
	sim.lk.RLock()
	defer sim.lk.RUnlock()

	// Get the wanted block keys as a set
	wantedKs := cid.NewSet()
	for _, b := range blks {
		c := b.Cid()
		if bc,ok:=b.(*blocks.CodedBlock); ok{
	//		fmt.Println("Debug: sim-split coded block")
			c = bc.Parent()
		}
		// For each session that is interested in the key
		for ses := range sim.wants[c] {
			// If the session wants the key (rather than just being interested)
			if wanted, ok := sim.wants[c][ses]; ok && wanted {
				// Add the key to the set
				wantedKs.Add(c)
	//			fmt.Println("Debug: sim-split added block")
			}
		}
	}

	// Separate the blocks into wanted and unwanted
	wantedBlks := make([]blocks.Block, 0, len(blks))
	notWantedBlks := make([]blocks.Block, 0)
	wantedBlksC := make([]*blocks.CodedBlock, 0)
	for _, b := range blks {
		if wantedKs.Has(b.Cid()) {
			wantedBlks = append(wantedBlks, b)
		} else if bc,ok:=b.(*blocks.CodedBlock); ok{
			if wantedKs.Has(bc.Parent()){
				wantedBlksC=append(wantedBlksC, bc)
			} else {
				notWantedBlks = append(notWantedBlks, b)
	//			fmt.Println("Debug: sim-split not wanted cblock", b.Cid())
			}
		} else {
			notWantedBlks = append(notWantedBlks, b)
	//		fmt.Println("Debug: sim-split not wanted block", b.Cid())
		}
	}
	//fmt.Println("Debug: sim-split", len(wantedBlks), len(notWantedBlks),len(wantedBlksC))
	return wantedBlks, notWantedBlks, wantedBlksC
}

// When the SessionManager receives a message it calls InterestedSessions() to
// find out which sessions are interested in the message.
func (sim *SessionInterestManager) InterestedSessions(blks []cid.Cid, haves []cid.Cid, dontHaves []cid.Cid, blksc map[cid.Cid]int) []uint64 {
	sim.lk.RLock()
	defer sim.lk.RUnlock()

	ks := make([]cid.Cid, 0, len(blks)+len(haves)+len(dontHaves)+len(blksc))
	ks = append(ks, blks...)
	ks = append(ks, haves...)
	ks = append(ks, dontHaves...)
	for k := range blksc {
		ks = append(ks, k)
	}

	// Create a set of sessions that are interested in the keys
	sesSet := make(map[uint64]struct{})
	for _, c := range ks {
		for s := range sim.wants[c] {
			sesSet[s] = struct{}{}
		}
	}

	// Convert the set into a list
	ses := make([]uint64, 0, len(sesSet))
	for s := range sesSet {
		ses = append(ses, s)
	}
	return ses
}

func (sim *SessionInterestManager) HasDecoder(cid cid.Cid) bool {
	sim.decodingkLk.Lock()
	defer sim.decodingkLk.Unlock()
	_, ok := sim.decoders[cid]
	return ok
}



func (sim *SessionInterestManager) SplitWantedUnwantedC(blks []*blocks.CodedBlock) ([]*blocks.CodedBlock, []*blocks.CodedBlock) {
	sim.decodingkLk.Lock()
	defer sim.decodingkLk.Unlock()

	wantedBlks := make([]*blocks.CodedBlock, 0, len(blks))
	notWantedBlks := make([]*blocks.CodedBlock, 0)

	for _, b := range blks{
		dc, ok := sim.decoders[b.Parent()]
		if !ok{
			notWantedBlks=append(notWantedBlks, b)
			fmt.Println("Debug: sim splitcoded - no decoder for", b.Parent())
			continue
		}
		if _, ok= sim.cwants[b.Parent()]; !ok{
			continue
		}
		//data := sim.decoderdata[b.Parent()]
		//dc.SetSymbolsStorage(&data)

		d := b.RawData()
		d = d[10:len(d)-4]
		r := dc.Rank()
		dc.ConsumePayload(&d)
		if dc.Rank() > r {
			wantedBlks = append(wantedBlks, b)
			want:=sim.cwants[b.Parent()]
			for ses, co:= range want{
				if co==1 {
					delete(want, ses)
					continue
				}
				want[ses]=co-1
			}
			if len(want)==0{
				delete(sim.cwants, b.Parent())
			}
		} else {
			notWantedBlks = append(notWantedBlks, b)
                        fmt.Println("Debug: sim-split: linear dependant block", b.Cid())
		}
		fmt.Println("Debug: sim-splitc: decoder ranks", r, dc.Rank())
	}
	return wantedBlks, notWantedBlks
}

func (sim *SessionInterestManager) IsCodedInterest(key cid.Cid) bool {
	sim.decodingkLk.Lock()
	defer sim.decodingkLk.Unlock()
	_	, ok := sim.cwants[key]
	return ok
}