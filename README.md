`cl-replica` is a library for hash-tables replication via an epidemic algorithm. 

## Introduction
Hash-tables are widely used and their replication might be particular interesting for distributed applications. However, keeping replicas consistent is not an easy task because one has to find a way to distribute local updates fast and reliably. These two requirements are contradictory, and a reasonable compromise can be found only for a particular use case. Our use case is a large heterogeneous unreliable network, which connects thousands of nodes. Each node is unstable and it keeps only a partial view of the network. It is clear, that a broadcast is not a good solution for two reasons. First, remote peers can temporarily vanish from the network and miss an update. Secondly, it would introduce _O(n)_ bottleneck at the node which shares its update. An elegant approach is to make use of epidemic algorithms. They are simple, stable and scale well [1-2].

## How it works
`cl-replica` uses the following epidemic protocol:
1. Every node keeps a partial view of its network environment
2. Every local update is shared with other nodes via "push-update"
3. Every node listens on udp/tcp ports and applies receiving updates locally
4. Every node provides periodically its neigbours with a cache with the keys last modified
5. Partial ordering is determined by vector clocks

Some implementation notes are summarized [here](ImplementationNotes.md).

All that is wrapped in an [API](./api.lisp) which is very close to the [standard](http://www.lispworks.com/documentation/HyperSpec/Front/index.htm) API for hash-tables:
* _share-hash-table (h-table &key this-node ... )_ creates a fresh replica from a hash-table
* _gethash-shared (key h-table-obj &optional default)_ retrieves value which corresponds to the key
* _newhash-shared (key h-table-obj new-value)_ adds new key-value pair
* _remhash-shared (key h-table-obj)_ removes key from replica
* _clrhash-shared (h-table-obj)_ deletes all key-value pairs from replica
* _maphash-shared (fn h-table-obj)_ iterates over key-value pairs and applies function _fn_ on them

Additionally, the function _destroy-shared-htable (h-table-obj wait-after-socket-is-closed)_ is provided which stops all network and data threads inside a _h-table-obj_ and returns a normal hash-table. 

## How to build
Configure ASDF to find `cl-replica` and execute:
```
(require "asdf")
(asdf:load-system "cl-replica")
(asdf:load-system "cl-replica/test") ;; to build unit-tests
```
## How to test
Put your unit-tests in [unit-tests](./unit-tests.lisp) and then execute in REPL:
```
CL-USER> (in-package :u-tests)
U-TESTS> (run-unit-tests)
```
## How to use
```
(defpackage :your-package
  (:use :cl :cl-replica))

(in-package :your-package)

(defun test (addr-1 addr-2)
  "Create two replicated hash tables"
  (let ((h1 (make-hash-table))
	(h2 (make-hash-table)))
    (let ((h-table-obj-1 (share-hash-table h1 :this-node addr-1
					      :other-nodes (list addr-2)))
	  (h-table-obj-2 (share-hash-table h2 :this-node addr-2
					      :other-nodes (list addr-1))))
      (newhash-shared 0 h-table-obj-1 "0") ;; add value locally
      (sleep 1)
      (unwind-protect
	   (progn
	     ;; check local value
	     (assert (string= "0" (gethash-shared 0 h-table-obj-1)))
	     ;; check replicated value on a remote peer
	     (assert (string= "0" (gethash-shared 0 h-table-obj-2))))
	(destroy-shared-htable h-table-obj-1 0)
	(destroy-shared-htable h-table-obj-2 0)))))

(test "tcp://127.0.0.1:5000" "tcp://127.0.0.1:5001")
(test "udp://127.0.0.1:5000" "udp://127.0.0.1:5001")
```
See the [API](./api.lisp) description and [unit-tests](./unit-tests.lisp) for numerous usage examples.
## Limitations:
`cl-replica` permits only commutative operations on hash-tables, i.e. serializabilty is not guaranteed. For the operations, which introduce dependencies among values, transactions are an obvious choice. Additionally, a lazy nature of data dissemination makes `cl-replica` inappropriate for an application with real-time requirements.

## Tested
SBCL 2.1.9

## Contribution:
Any kind of contributions are highly appreciated. Two obvious ways to improve the library are (1) to make it portable across different Lisp compilers and (2) to test it in a real distributed environment. Please, take into account the following guidilines:
1. Write unit tests for your code and put it in [unit-tests](./unit-tests.lisp)
2. When applicable, do not use third-party libraries and make extensive use of compiler features.
3. Compile your sources with `(declaim (optimize (debug 3)))` flag.

## References
[1] [Demers et al., Epidemic Algorithms for Replicated Database Maintenance, 1989](http://bitsavers.informatik.uni-stuttgart.de/pdf/xerox/parc/techReports/CSL-89-1_Epidemic_Algorithms_for_Replicated_Database_Maintenance.pdf) <br />
[2] [Holliday et al., Epidemic Algorithms for Replicated Databases, 2003](https://www.researchgate.net/publication/3297201_Epidemic_algorithms_for_replicated_databases) 

