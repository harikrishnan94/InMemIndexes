# Concurrent In-memory Indexes
## Concurrent Btree

Concurrent Btree's concurrency control is based on [Optimistic locking](https://db.in.tum.de/~leis/papers/artsync.pdf) principle. Salient features of this concurrency control are

  - Very simple - minimal changes to code to support concurrency
  - Good scalability 

### References 
* [OLFIT (optimistic, latch-free index traversal) Concurrency control](https://pdfs.semanticscholar.org/c964/691f3cb8f86a19d17a3beed2f50444df4669.pdf)
