/**
 *  @file
 *  @copyright defined in aergo/LICENSE.txt
 */

package trie

import (
	"bytes"
)

// MerkleProof creates a merkle proof for a key in the latest trie
// A non inclusion proof is a proof to a default value
func (s *SMT) MerkleProof(key []byte) ([][]byte, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.merkleProof(s.Root, s.TrieHeight, 0, key, nil)
}

// MerkleProofCompressed returns a compressed merkle proof.
// The proof contains a bitmap of non default hashes and the non default hashes.
func (s *SMT) MerkleProofCompressed(key []byte) ([]byte, [][]byte, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	bitmap := make([]byte, s.TrieHeight/8)
	mp, err := s.merkleProofCompressed(s.Root, s.TrieHeight, 0, key, bitmap, nil)
	return bitmap, mp, err
}

// MerkleProofCompressed2 returns a compressed merkle proof like MerkleProofCompressed
// This version 1st calls MerkleProof and then removes the default nodes.
func (s *SMT) MerkleProofCompressed2(key []byte) ([]byte, [][]byte, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	// create a regular merkle proof and then compress it
	mpFull, err := s.merkleProof(s.Root, s.TrieHeight, 0, key, nil)
	if err != nil {
		return nil, nil, err
	}
	var mp [][]byte
	bitmap := make([]byte, s.TrieHeight/8)
	for i, node := range mpFull {
		if !bytes.Equal(node, s.defaultHashes[i]) {
			bitSet(bitmap, i)
			mp = append(mp, node)
		}
	}
	return bitmap, mp, nil
}

// merkleProof generates a Merke proof of inclusion or non inclusion for a given trie root
func (s *SMT) merkleProof(root []byte, height, iBatch int, key []byte, batch [][]byte) ([][]byte, error) {
	if height == 0 {
		return nil, nil
	}
	// Fetch the children of the node
	batch, iBatch, lnode, rnode, isShortcut, err := s.loadChildren(root, height, iBatch, batch)
	if err != nil {
		return nil, err
	}
	if isShortcut {
		// append all default hashes down the tree
		if bytes.Equal(lnode[:HashLength], key) {
			rest := make([][]byte, height)
			copy(rest, s.defaultHashes[:height]) // needed because append will modify underlying array
			return rest, nil
		}
		// if the key is empty, unroll until it diverges from the shortcut key and add the non default node
		return s.unrollShortcutAndKey(lnode[:HashLength], rnode[:HashLength], height, key), nil
	}

	// append the left or right node to the proof
	if bitIsSet(key, s.TrieHeight-height) {
		mp, err := s.merkleProof(rnode, height-1, 2*iBatch+2, key, batch)
		if err != nil {
			return nil, err
		}
		if len(lnode) != 0 {
			return append(mp, lnode[:HashLength]), nil
		} else {
			return append(mp, s.defaultHashes[height-1]), nil
		}
	}
	mp, err := s.merkleProof(lnode, height-1, 2*iBatch+1, key, batch)
	if err != nil {
		return nil, err
	}
	if len(rnode) != 0 {
		return append(mp, rnode[:HashLength]), nil
	} else {
		return append(mp, s.defaultHashes[height-1]), nil
	}
}

// merkleProofCompressed generates a Merke proof of inclusion or non inclusion for a given trie root
// a proof node is only appended if it is non default and the corresponding bit is set in the bitmap
func (s *SMT) merkleProofCompressed(root []byte, height, iBatch int, key []byte, bitmap []byte, batch [][]byte) ([][]byte, error) {
	if height == 0 {
		return nil, nil
	}
	// Fetch the children of the node
	batch, iBatch, lnode, rnode, isShortcut, err := s.loadChildren(root, height, iBatch, batch)
	if err != nil {
		return nil, err
	}
	if isShortcut {
		if bytes.Equal(lnode[:HashLength], key) {
			return nil, nil
		}
		// if the key is empty, unroll until it diverges from the shortcut key and add the non default node
		return [][]byte{s.unrollShortcutAndKeyCompressed(lnode[:HashLength], rnode[:HashLength], height, bitmap, key)}, nil
	}

	// append the left or right node to the proof, if it is non default and set bitmap
	if bitIsSet(key, s.TrieHeight-height) {
		//if !bytes.Equal(lnode, s.defaultHashes[height-1]) {
		if len(lnode) != 0 {
			// with validate proof, use a default hash when bit is not set
			bitSet(bitmap, height-1)
			mp, err := s.merkleProofCompressed(rnode, height-1, 2*iBatch+2, key, bitmap, batch)
			if err != nil {
				return nil, err
			}
			return append(mp, lnode[:HashLength]), nil
		}
		mp, err := s.merkleProofCompressed(rnode, height-1, 2*iBatch+2, key, bitmap, batch)
		if err != nil {
			return nil, err
		}
		return mp, nil
	}
	if len(rnode) != 0 {
		//if !bytes.Equal(rnode, s.defaultHashes[height-1]) {
		// with validate proof, use a default hash when bit is not set
		bitSet(bitmap, height-1)
		mp, err := s.merkleProofCompressed(lnode, height-1, 2*iBatch+1, key, bitmap, batch)
		if err != nil {
			return nil, err
		}
		return append(mp, rnode[:HashLength]), nil
	}
	mp, err := s.merkleProofCompressed(lnode, height-1, 2*iBatch+1, key, bitmap, batch)
	if err != nil {
		return nil, err
	}
	return mp, err
}

// VerifyMerkleProof verifies that key/value is included in the trie with latest root
func (s *SMT) VerifyMerkleProof(ap [][]byte, key, value []byte) bool {
	return bytes.Equal(s.Root, s.verifyMerkleProof(ap, s.TrieHeight, key, value))
}

// VerifyMerkleProofCompressed verifies that key/value is included in the trie with latest root
func (s *SMT) VerifyMerkleProofCompressed(bitmap []byte, ap [][]byte, key, value []byte) bool {
	return bytes.Equal(s.Root, s.verifyMerkleProofCompressed(bitmap, ap, s.TrieHeight, len(ap), key, value))
}

// verifyMerkleProof verifies that a key/value is included in the trie with given root
func (s *SMT) verifyMerkleProof(ap [][]byte, height int, key, value []byte) []byte {
	if height == 0 {
		return value
	}
	if bitIsSet(key, s.TrieHeight-height) {
		return s.hash(ap[height-1], s.verifyMerkleProof(ap, height-1, key, value))
	}
	return s.hash(s.verifyMerkleProof(ap, height-1, key, value), ap[height-1])
}

// verifyMerkleProof verifies that a key/value is included in the trie with given root
func (s *SMT) verifyMerkleProofCompressed(bitmap []byte, ap [][]byte, height int, apIndex int, key, value []byte) []byte {
	if height == 0 {
		return value
	}
	if bitIsSet(key, s.TrieHeight-height) {
		if bitIsSet(bitmap, height-1) {
			return s.hash(ap[apIndex-1], s.verifyMerkleProofCompressed(bitmap, ap, height-1, apIndex-1, key, value))
		}
		return s.hash(s.defaultHashes[height-1], s.verifyMerkleProofCompressed(bitmap, ap, height-1, apIndex, key, value))

	}
	if bitIsSet(bitmap, height-1) {
		return s.hash(s.verifyMerkleProofCompressed(bitmap, ap, height-1, apIndex-1, key, value), ap[apIndex-1])
	}
	return s.hash(s.verifyMerkleProofCompressed(bitmap, ap, height-1, apIndex, key, value), s.defaultHashes[height-1])
}

// shortcutToSubTreeRoot computes the subroot at height of a subtree containing one key
func (s *SMT) shortcutToSubTreeRoot(key, value []byte, height int) []byte {
	if height == 0 {
		return value
	}
	if bitIsSet(key, s.TrieHeight-height) {
		return s.hash(s.defaultHashes[height-1], s.shortcutToSubTreeRoot(key, value, height-1))
	}
	return s.hash(s.shortcutToSubTreeRoot(key, value, height-1), s.defaultHashes[height-1])
}

// unrollShortcutAndKey returns the merkle proof nodes of an empty key in a subtree that contains another key
// the key we are proving is not in the tree, it's value is default
func (s *SMT) unrollShortcutAndKey(key, value []byte, height int, emptyKey []byte) [][]byte {
	// if the keys have the same bits add a default hash to the proof
	if bitIsSet(key, s.TrieHeight-height) == bitIsSet(emptyKey, s.TrieHeight-height) {
		return append(s.unrollShortcutAndKey(key, value, height-1, emptyKey), s.defaultHashes[height-1])
	}
	// if the keys diverge, calculate the non default subroot and add default hashes until the leaf
	rest := make([][]byte, height-1)
	copy(rest, s.defaultHashes[:height-1])
	return append(rest, s.shortcutToSubTreeRoot(key, value, height-1))
}

// unrollShortcutAndKeyCompressed returns the merkle proof nodes of an empty key in a subtree that contains another key
// the key we are proving is not in the tree, it's value is default
func (s *SMT) unrollShortcutAndKeyCompressed(key, value []byte, height int, bitmap []byte, emptyKey []byte) []byte {
	// this version of unroll for compressed proofs simply sets the bitmap for non default nodes
	if bitIsSet(key, s.TrieHeight-height) == bitIsSet(emptyKey, s.TrieHeight-height) {
		return s.unrollShortcutAndKeyCompressed(key, value, height-1, bitmap, emptyKey)
	}
	bitSet(bitmap, height-1)
	return s.shortcutToSubTreeRoot(key, value, height-1)
}
