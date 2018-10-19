# Sparse Merkle Tree

A performance oriented implementation of a binary SMT with parallel update, node batching and storage shortcuts.

Details of the SMT implementation : [https://medium.com/@ouvrard.pierre.alain/sparse-merkle-tree-86e6e2fc26da](https://medium.com/@ouvrard.pierre.alain/sparse-merkle-tree-86e6e2fc26da)

### Features
- Efficient Merkle proof verification (**binary** tree structure)
- Compressed Merkle proofs
- Efficient database reads and storage (**node batching**)
- Reduced data storage (**shortcut nodes** for subtrees containing one key)
- Simultaneous update of multiple keys with goroutines


