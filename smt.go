/**
 *  @file
 *  @copyright defined in aergo/LICENSE.txt
 */

package trie

// The Package Trie implements a sparse merkle trie.

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/aergoio/aergo-lib/db"
)

// TODO make a secure trie that hashes keys with a random seed to be sure the trie is sparse.

// SMT is a sparse Merkle tree.
type SMT struct {
	db *CacheDB
	// Root is the current root of the smt.
	Root []byte
	// lock is for the whole struct
	lock sync.RWMutex
	hash func(data ...[]byte) []byte
	// KeySize is the size in bytes corresponding to TrieHeight, is size of an address.
	KeySize       uint64
	TrieHeight    uint64
	defaultHashes [][]byte
	// LoadDbCounter counts the nb of db reads in on update
	LoadDbCounter uint64
	// loadDbMux is a lock for LoadDbCounter
	loadDbMux sync.RWMutex
	// LoadCacheCounter counts the nb of cache reads in on update
	LoadCacheCounter uint64
	// liveCountMux is a lock fo LoadCacheCounter
	liveCountMux sync.RWMutex
	// counterOn is used to enable/diseable for efficiency
	counterOn bool
	// CacheHeightLimit is the number of tree levels we want to store in cache
	CacheHeightLimit uint64
	// pastTries stores the past maxPastTries trie roots to revert
	pastTries [][]byte
}

// NewSMT creates a new SMT given a keySize and a hash function.
func NewSMT(keySize uint64, hash func(data ...[]byte) []byte, store db.DB) *SMT {
	s := &SMT{
		hash:             hash,
		TrieHeight:       keySize * 8,
		CacheHeightLimit: 232,     //246, //234, // based on the number of nodes we can keep in memory.
		KeySize:          keySize, // same length as hash output
		counterOn:        false,
	}
	s.db = &CacheDB{
		liveCache:    make(map[Hash][][]byte, 5e6),
		updatedNodes: make(map[Hash][][]byte, 5e3),
		store:        store,
	}
	s.Root = nil
	s.loadDefaultHashes()
	return s
}

// loadDefaultHashes creates the default hashes
func (s *SMT) loadDefaultHashes() {
	s.defaultHashes = make([][]byte, s.TrieHeight+1)
	s.defaultHashes[0] = DefaultLeaf
	var h []byte
	for i := 1; i <= int(s.TrieHeight); i++ {
		h = s.hash(s.defaultHashes[i-1], s.defaultHashes[i-1])
		s.defaultHashes[i] = h
	}
}

// Update adds a sorted list of keys and their values to the trie
func (s *SMT) Update(keys, values [][]byte) ([]byte, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.LoadDbCounter = 0
	s.LoadCacheCounter = 0

	ch := make(chan result, 1)
	s.update(s.Root, keys, values, nil, 0, s.TrieHeight, false, true, ch)
	result := <-ch
	if result.err != nil {
		return nil, result.err
	}
	s.Root = result.update[:HashLength]
	return s.Root, nil
}

// result is used to contain the result of goroutines and is sent through a channel.
type result struct {
	update []byte
	err    error
}

// update adds a sorted list of keys and their values to the trie.
// It returns the root of the updated tree.
func (s *SMT) update(root []byte, keys, values, batch [][]byte, iBatch uint8, height uint64, shortcut, store bool, ch chan<- (result)) {
	if height == 0 {
		if bytes.Equal(values[0], DefaultLeaf) {
			ch <- result{nil, nil}
		} else {
			ch <- result{values[0], nil}
		}
		return
	}
	batch, iBatch, lnode, rnode, isShortcut, err := s.loadChildren(root, height, batch, iBatch)
	if err != nil {
		ch <- result{nil, err}
		return
	}
	if isShortcut {
		keys, values = s.maybeAddShortcutToKV(keys, values, lnode[:HashLength], rnode[:HashLength])
		// The shortcut node was added to keys and values so consider this subtree default.
		lnode, rnode = nil, nil
		// update in the batch (set key, value to default to the next loadChildren is correct)
		batch[2*iBatch+1] = lnode
		batch[2*iBatch+2] = rnode
	}

	// Split the keys array so each branch can be updated in parallel
	lkeys, rkeys := s.splitKeys(keys, s.TrieHeight-height)
	splitIndex := len(lkeys)
	lvalues, rvalues := values[:splitIndex], values[splitIndex:]

	if shortcut {
		store = false    //stop storing only after the shortcut node.
		shortcut = false // remove shortcut node flag
	}
	if (len(lnode) == 0) && (len(rnode) == 0) && (len(keys) == 1) && store {
		if !bytes.Equal(values[0], DefaultLeaf) {
			shortcut = true
		} else {
			// if the subtree contains only one key, store the key/value in a shortcut node
			store = false
		}
	}
	switch {
	case len(lkeys) == 0 && len(rkeys) > 0:
		s.updateRight(lnode, rnode, root, keys, values, batch, iBatch, height, shortcut, store, ch)
	case len(lkeys) > 0 && len(rkeys) == 0:
		s.updateLeft(lnode, rnode, root, keys, values, batch, iBatch, height, shortcut, store, ch)
	default:
		s.updateParallel(lnode, rnode, root, keys, values, batch, lkeys, rkeys, lvalues, rvalues, iBatch, height, shortcut, store, ch)
	}
}

// updateParallel updates both sides of the trie simultaneously
func (s *SMT) updateParallel(lnode, rnode, root []byte, keys, values, batch, lkeys, rkeys, lvalues, rvalues [][]byte, iBatch uint8, height uint64, shortcut, store bool, ch chan<- (result)) {
	// keys are separated between the left and right branches
	// update the branches in parallel
	lch := make(chan result, 1)
	rch := make(chan result, 1)
	go s.update(lnode, lkeys, lvalues, batch, 2*iBatch+1, height-1, shortcut, store, lch)
	go s.update(rnode, rkeys, rvalues, batch, 2*iBatch+2, height-1, shortcut, store, rch)
	lresult := <-lch
	rresult := <-rch
	if lresult.err != nil {
		ch <- result{nil, lresult.err}
		return
	}
	if rresult.err != nil {
		ch <- result{nil, rresult.err}
		return
	}
	ch <- result{s.interiorHash(lresult.update, rresult.update, height-1, root, shortcut, store, keys, values, batch, iBatch), nil}

}

// updateRight updates the right side of the tree
func (s *SMT) updateRight(lnode, rnode, root []byte, keys, values, batch [][]byte, iBatch uint8, height uint64, shortcut, store bool, ch chan<- (result)) {
	// all the keys go in the right subtree
	newch := make(chan result, 1)
	s.update(rnode, keys, values, batch, 2*iBatch+2, height-1, shortcut, store, newch)
	res := <-newch
	if res.err != nil {
		ch <- result{nil, res.err}
		return
	}
	ch <- result{s.interiorHash(lnode, res.update, height-1, root, shortcut, store, keys, values, batch, iBatch), nil}
}

// updateLeft updates the left side of the tree
func (s *SMT) updateLeft(lnode, rnode, root []byte, keys, values, batch [][]byte, iBatch uint8, height uint64, shortcut, store bool, ch chan<- (result)) {
	// all the keys go in the left subtree
	newch := make(chan result, 1)
	s.update(lnode, keys, values, batch, 2*iBatch+1, height-1, shortcut, store, newch)
	res := <-newch
	if res.err != nil {
		ch <- result{nil, res.err}
		return
	}
	ch <- result{s.interiorHash(res.update, rnode, height-1, root, shortcut, store, keys, values, batch, iBatch), nil}
}

// splitKeys devides the array of keys into 2 so they can update left and right branches in parallel
func (s *SMT) splitKeys(keys [][]byte, height uint64) ([][]byte, [][]byte) {
	for i, key := range keys {
		if bitIsSet(key, height) {
			return keys[:i], keys[i:]
		}
	}
	return keys, nil
}

// maybeAddShortcutToKV adds a shortcut key to the keys array to be updated.
// this is used when a subtree containing a shortcut node is being updated
func (s *SMT) maybeAddShortcutToKV(keys, values [][]byte, shortcutKey, shortcutVal []byte) ([][]byte, [][]byte) {
	newKeys := make([][]byte, 0, len(keys)+1)
	newVals := make([][]byte, 0, len(keys)+1)

	if bytes.Compare(shortcutKey, keys[0]) < 0 {
		newKeys = append(newKeys, shortcutKey)
		newKeys = append(newKeys, keys...)
		newVals = append(newVals, shortcutVal)
		newVals = append(newVals, values...)
	} else if bytes.Compare(shortcutKey, keys[len(keys)-1]) > 0 {
		newKeys = append(newKeys, keys...)
		newKeys = append(newKeys, shortcutKey)
		newVals = append(newVals, values...)
		newVals = append(newVals, shortcutVal)
	} else {
		higher := false
		for i, key := range keys {
			if bytes.Equal(shortcutKey, key) {
				// the shortcut keys is being updated
				return keys, values
			}
			if bytes.Compare(shortcutKey, key) > 0 {
				higher = true
			}
			if higher && bytes.Compare(shortcutKey, key) < 0 {
				// insert shortcut in slices
				newKeys = append(newKeys, keys[:i]...)
				newKeys = append(newKeys, shortcutKey)
				newKeys = append(newKeys, keys[i:]...)
				newVals = append(newVals, values[:i]...)
				newVals = append(newVals, shortcutVal)
				newVals = append(newVals, values[i:]...)
			}
		}
	}
	return newKeys, newVals
}

// loadChildren looks for the children of a node.
// if the node is not stored in cache, it will be loaded from db.
func (s *SMT) loadChildren(root []byte, height uint64, batch [][]byte, iBatch uint8) ([][]byte, uint8, []byte, []byte, bool, error) {
	isShortcut := false
	if height%4 == 0 {
		if len(root) == 0 {
			// create a new default batch
			batch = make([][]byte, 31, 31)
			batch[0] = []byte{0}
		} else {
			var err error
			batch, err = s.loadBatch(root[:HashLength])
			if err != nil {
				return nil, 0, nil, nil, false, err
			}
		}
		iBatch = 0
		if batch[0][0] == 1 {
			isShortcut = true
		}
	} else {
		if len(batch[iBatch]) != 0 && batch[iBatch][HashLength] == 1 {
			isShortcut = true
		}
	}
	return batch, iBatch, batch[2*iBatch+1], batch[2*iBatch+2], isShortcut, nil
}

// loadBatch fetches a batch of nodes in cache or db
func (s *SMT) loadBatch(root []byte) ([][]byte, error) {
	var node Hash
	copy(node[:], root)
	s.db.liveMux.RLock()
	val, exists := s.db.liveCache[node]
	s.db.liveMux.RUnlock()
	if exists {
		if s.counterOn {
			s.liveCountMux.Lock()
			s.LoadCacheCounter++
			s.liveCountMux.Unlock()
		}
		// TODO if all values are diffent (ie the RLP contains the key)
		// then this copy operation is unnecessary.
		// but if 2 subtrees are the same, modifying one, will unwillingly
		// modify the other.
		//return val, nil
		newVal := make([][]byte, 31, 31)
		copy(newVal, val)
		return newVal, nil
	}
	// checking updated nodes is useful if get() or update() is called twice in a row without db commit
	s.db.updatedMux.RLock()
	val, exists = s.db.updatedNodes[node]
	s.db.updatedMux.RUnlock()
	if exists {
		// TODO same as above
		// return val, nil
		newVal := make([][]byte, 31, 31)
		copy(newVal, val)
		return newVal, nil
	}
	//Fetch node in disk database
	if s.db.store == nil {
		return nil, fmt.Errorf("DB not connected to trie")
	}
	if s.counterOn {
		s.loadDbMux.Lock()
		s.LoadDbCounter++
		s.loadDbMux.Unlock()
	}
	s.db.lock.RLock()
	dbval := s.db.store.Get(root)
	s.db.lock.RUnlock()
	nodeSize := len(dbval)
	if nodeSize != 0 {
		return s.parseBatch(dbval), nil
	}
	return nil, fmt.Errorf("the trie node %x is unavailable in the disk db, db may be corrupted", root)
}

// parseBatch decodes the byte data into a slice of nodes and bitmap
func (s *SMT) parseBatch(val []byte) [][]byte {
	batch := make([][]byte, 31, 31)
	bitmap := val[:4]
	// check if the batch root is a shortcut
	if bitIsSet(val, 31) {
		batch[0] = []byte{1}
		batch[1] = val[4 : 4+33]
		batch[2] = val[4+33 : 4+33*2]
	} else {
		batch[0] = []byte{0}
		j := 0
		for i := 1; i <= 30; i++ {
			if bitIsSet(bitmap, uint64(i-1)) {
				batch[i] = val[4+33*j : 4+33*(j+1)]
				j++
			}
		}
	}
	return batch
}

// Get fetches the value of a key by going down the current trie root.
func (s *SMT) Get(key []byte) ([]byte, error) {
	return s.get(append(s.Root, byte(0)), key, nil, 0, s.TrieHeight)
}

// get fetches the value of a key given a trie root
func (s *SMT) get(root []byte, key []byte, batch [][]byte, iBatch uint8, height uint64) ([]byte, error) {
	if height == 0 {
		if len(root) == 0 {
			return nil, nil
		}
		return root[:HashLength], nil
	}
	// Fetch the children of the node
	batch, iBatch, lnode, rnode, isShortcut, err := s.loadChildren(root, height, batch, iBatch)
	if err != nil {
		return nil, err
	}
	if isShortcut {
		if bytes.Equal(lnode[:HashLength], key) {
			return rnode[:HashLength], nil
		}
		return nil, nil
	}
	if bitIsSet(key, s.TrieHeight-height) {
		return s.get(rnode, key, batch, 2*iBatch+2, height-1)
	}
	return s.get(lnode, key, batch, 2*iBatch+1, height-1)
}

// DefaultHash is a getter for the defaultHashes array
func (s *SMT) DefaultHash(height uint64) []byte {
	return s.defaultHashes[height]
}

// interiorHash hashes 2 children to get the parent hash and stores it in the updatedNodes and maybe in liveCache.
// the key is the hash and the value is the appended child nodes or the appended key/value in case of a shortcut.
// keys of go mappings cannot be byte slices so the hash is copied to a byte array
func (s *SMT) interiorHash(left, right []byte, height uint64, oldRoot []byte, shortcut, store bool, keys, values, batch [][]byte, iBatch uint8) []byte {
	var h []byte
	if (len(left) == 0) && (len(right)) == 0 {
		// if a key was deleted, the node becomes default
		batch[2*iBatch+1] = left
		batch[2*iBatch+2] = right
		return nil
	} else if len(left) == 0 {
		h = s.hash(s.defaultHashes[height], right[:HashLength])
	} else if len(right) == 0 {
		h = s.hash(left[:HashLength], s.defaultHashes[height])
	} else {
		h = s.hash(left[:HashLength], right[:HashLength])
	}
	if !store {
		// a shortcut node cannot move up
		return append(h, byte(0))
	}
	if !shortcut {
		h = append(h, byte(0))
	} else {
		// store the value at the shortcut node instead of height 0.
		h = append(h, byte(1))
		left = append(keys[0], byte(2))
		right = append(values[0], byte(2))
	}
	batch[2*iBatch+2] = right
	batch[2*iBatch+1] = left

	/*
		if len(batch[2*iBatch+2]) == 0 {
			if len(right) != 0 {
				batch[2*iBatch+2] = append(batch[2*iBatch+2], right...)
			}
		} else {
			if len(right) == 0 {
				batch[2*iBatch+2] = nil
			} else {
				copy(batch[2*iBatch+2], right)
			}
		}
		if len(batch[2*iBatch+1]) == 0 {
			if len(left) != 0 {
				batch[2*iBatch+1] = append(batch[2*iBatch+1], left...)
			}
		} else {
			if len(left) == 0 {
				batch[2*iBatch+1] = nil
			} else {
				copy(batch[2*iBatch+1], left)
			}
		}
	*/

	// maybe store batch node
	if (height+1)%4 == 0 {
		var node Hash
		copy(node[:], h[:HashLength])
		if shortcut {
			batch[0] = []byte{1}
		} else {
			batch[0] = []byte{0}
		}
		// if node is default, interior hash returned at the begining
		s.db.updatedMux.Lock()
		s.db.updatedNodes[node] = batch
		s.db.updatedMux.Unlock()
		if height > s.CacheHeightLimit {
			s.db.liveMux.Lock()
			s.db.liveCache[node] = batch
			if (len(oldRoot) != 0) && !bytes.Equal(h, oldRoot) {
				// Delete old liveCache node if it has been updated and is not default
				var node Hash
				copy(node[:], oldRoot[:HashLength])
				delete(s.db.liveCache, node)
				//NOTE this could delete a node used by another part of the tree if some values are equal.
				// not the case if the key is hashed with the value
			}
			s.db.liveMux.Unlock()
		}
	}
	return h
}

// Commit stores the updated nodes to disk
func (s *SMT) Commit() error {
	if s.db.store == nil {
		return fmt.Errorf("DB not connected to trie")
	}
	// Commit the new nodes to database, clear updatedNodes and store the Root in history for reverts.
	if len(s.pastTries) >= maxPastTries {
		copy(s.pastTries, s.pastTries[1:])
		s.pastTries[len(s.pastTries)-1] = s.Root
	} else {
		s.pastTries = append(s.pastTries, s.Root)
	}
	s.db.commit()
	s.db.updatedNodes = make(map[Hash][][]byte, len(s.db.updatedNodes)*2)
	return nil
}
