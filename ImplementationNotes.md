## Rationale
The theory says that a rumor mongering backed-up by anti-entropy infect eventually the entire population and that the process is efficient and scalable. While the former is a mathematically proven fact, that latter is implementation dependent. Since there are myriads of ways to implement epidemic protocols, the efficiency should not be taken for granted. Here we summarize several key implementation ideas.

### Architecture
<p align="center">
	<img src=library_layout.png width=50% height=50%>
</p>

### Push vs pull vs push-pull.
A _push-update_ sends a new value to another node, then that node checks the timestamp of this update and accepts it if this value is _newer_ than a local copy. A _pull-update_ first fetches the timestamp of the value from a remote peer, and if the local value is newer, it gets shipped. A _push-pull-update_ is a combination of two. <br />
Authors from [1] argue that _pull_ and _push-pull_ model show much better infection rates than the _push_ approach. This might be true in theory, but typical network mechanics favors the _push_ model because

1. _push-update_ is a one-step process, whereas a _pull-update_ must first retrieve information from other nodes about the updated value.
2. _push-updates_ match well the fire-and-forget philosophy of udp sockets.
3. results in [2] show that the difference is not that big

Also, if tcp sockets are used, one doesn't need a confirmation that an update has been seen, as it was suggested by the authors in [1]. This is the reason why `cl-replica` uses _push-updates_ with udp/tcp sockets.

### Data races & timestamps
Any distributed environment suffers from the lack of absolute time. A usual way to determine partial ordering of events is to use vector clocks. Every value sent from one node to another has a vector timestamp and comparison of these vectors allows us to reason about causality. However, the vector clocks cannot resolve data races, they can only detect them. What happens if two concurrent events occur is left to an application. `cl-replica` resolves data races by comparing absolute values of the corresponding vectors. A clock, whose vector is bigger, wins because its time either has been seen by a larger number of nodes or it reflects more events.

### Checksums & incremental synchronization
The original paper [1] suggests to use a checksum of a hash-table as a measure of their (in)equality. An anti-entropy data exchange stops when both hash-tables have the same checksums. This raises an important question: how to calculate the checksum of a hash-table? The best option is to use its memory representation because a computation of the checksum of a large byte array is straightforward. Unfortunately, in garbage-collected languages this is not possible. See the following [stackoverflow thread](https://stackoverflow.com/questions/69974963/object-memory-layout-in-common-lisp).  One (bad) workaround is to retrieve all key-value pairs, calculate their hashes and take the sum. This should work under the assumption that a sum of a large number of hashes uniquely represents a data set. But it would be a definite performance bottleneck unless two requirements are met:
1. the number of nodes N >> 1
2. the network is uniform

In this case the whole system is not affected because while these two nodes perform a long-running data exchange, the others keep infecting each other. However, it is easy to see how these requirements can be locally violated. If a network has a topological bottleneck, for example a small number of nodes which connect two big network clusters, the local data exchange would block this _bridge_ and the clusters get disjointed.
This is exactly the reason why `cl-replica` does not perform long-running synchronizations and computations of hash-table checksums. Instead, each node receives periodically a list with the key-value pairs (a.k.a. _cache_), which have been modified last. An application can decide how big the cache should be. 

## References
[1] [Demers et al., Epidemic Algorithms for Replicated Database Maintenance, 1989](http://bitsavers.informatik.uni-stuttgart.de/pdf/xerox/parc/techReports/CSL-89-1_Epidemic_Algorithms_for_Replicated_Database_Maintenance.pdf)   <br />
[2] Maarten van Steen, Graph Theory and Complex Networks, 2010

