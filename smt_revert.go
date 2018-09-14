/**
 *  @file
 *  @copyright defined in aergo/LICENSE.txt
 */

package trie

import (
	"bytes"
	"fmt"
)

// Revert rewinds the state tree to a previous version
// All the nodes (subtree roots and values) reverted are deleted from the database.
func (s *SMT) Revert(toOldRoot []byte) error {
	if bytes.Equal(s.Root, toOldRoot) {
		return fmt.Errorf("Trying to revers to the same root %x", s.Root)
	}
	//check if toOldRoot is in s.pastTries
	canRevert := false
	toIndex := 0
	for i, r := range s.pastTries {
		if bytes.Equal(r, toOldRoot) {
			canRevert = true
			toIndex = i
			break
		}
	}
	if !canRevert {
		return fmt.Errorf("The root is not contained in the cached tries, too old to be reverted : %x", s.Root)
	}

	// For every node of toOldRoot, compare it to the equivalent node in other pasttries between toOldRoot and current s.Root. If a node is different, delete the one from pasttries
	s.db.nodesToRevert = make([][]byte, 0)
	for i := toIndex + 1; i < len(s.pastTries); i++ {
		ch := make(chan error, 1)
		s.maybeDeleteSubTree(toOldRoot, s.pastTries[i], s.TrieHeight, nil, nil, 0, ch)
		err := <-ch
		if err != nil {
			return err
		}
	}
	// NOTE The tx interface doesnt handle ErrTxnTooBig
	txn := s.db.store.NewTx(true)
	//for _, key := range toBeDeleted {
	for _, key := range s.db.nodesToRevert {
		txn.Delete(key[:HashLength])
	}
	txn.Commit()

	s.pastTries = s.pastTries[:toIndex+1]
	s.Root = toOldRoot
	// load default hashes in live cache
	s.db.liveCache = make(map[Hash][][]byte)
	return nil
}

// maybeDeleteSubTree compares the subtree nodes of 2 tries and keeps only the older one
func (s *SMT) maybeDeleteSubTree(original []byte, maybeDelete []byte, height uint64, batch, batch2 [][]byte, iBatch uint8, ch chan<- (error)) {
	if bytes.Equal(original, maybeDelete) || len(maybeDelete) == 0 {
		ch <- nil
		return
	}
	if height == 0 {
		ch <- nil
		return
	}

	// if this point os reached, then the root of the batch is same
	// so the batch is also same.
	batch, iBatch, lnode, rnode, isShortcut, lerr := s.loadChildren(original, height, batch, iBatch)
	if lerr != nil {
		ch <- lerr
		return
	}
	batch2, _, lnode2, rnode2, isShortcut2, rerr := s.loadChildren(maybeDelete, height, batch2, iBatch)
	if rerr != nil {
		ch <- rerr
		return
	}

	if isShortcut != isShortcut2 {
		if isShortcut {
			ch1 := make(chan error, 1)
			s.deleteSubTree(maybeDelete, height, batch2, iBatch, ch1)
			err := <-ch1
			if err != nil {
				ch <- err
				return
			}
		} else if iBatch == 0 {
			s.deleteRevertedNode(maybeDelete)
		}
	} else {
		if isShortcut {
			if !bytes.Equal(lnode, lnode2) || !bytes.Equal(rnode, rnode2) {
				if iBatch == 0 {
					s.deleteRevertedNode(maybeDelete)
				}
			}
		} else {
			// Delete subtree if not equal
			if iBatch == 0 {
				s.deleteRevertedNode(maybeDelete)
			}
			ch1 := make(chan error, 1)
			ch2 := make(chan error, 1)
			go s.maybeDeleteSubTree(lnode, lnode2, height-1, batch, batch2, 2*iBatch+1, ch1)
			go s.maybeDeleteSubTree(rnode, rnode2, height-1, batch, batch2, 2*iBatch+2, ch2)
			err1 := <-ch1
			err2 := <-ch2
			if err1 != nil {
				ch <- err1
				return
			}
			if err2 != nil {
				ch <- err2
				return
			}
		}
	}
	ch <- nil
	return
}

// deleteSubTree deletes all the nodes contained in a tree
func (s *SMT) deleteSubTree(root []byte, height uint64, batch [][]byte, iBatch uint8, ch chan<- (error)) {
	if height == 0 || len(root) == 0 {
		ch <- nil
		return
	}
	batch, iBatch, lnode, rnode, isShortcut, err := s.loadChildren(root, height, batch, iBatch)
	if err != nil {
		ch <- err
		return
	}
	if !isShortcut {
		ch1 := make(chan error, 1)
		ch2 := make(chan error, 1)
		go s.deleteSubTree(lnode, height-1, batch, 2*iBatch+1, ch1)
		go s.deleteSubTree(rnode, height-1, batch, 2*iBatch+2, ch2)
		lerr := <-ch1
		rerr := <-ch2
		if lerr != nil {
			ch <- lerr
			return
		}
		if rerr != nil {
			ch <- rerr
			return
		}
	}
	if iBatch == 0 {
		s.deleteRevertedNode(root)
	}
	ch <- nil
	return
}

// deleteRevertedNode adds the node to updatedNodes to be reverted
func (s *SMT) deleteRevertedNode(root []byte) {
	s.db.revertMux.Lock()
	s.db.nodesToRevert = append(s.db.nodesToRevert, root)
	s.db.revertMux.Unlock()
}
