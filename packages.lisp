(defpackage :cl-replica.vector-clock
  (:use :cl)
  (:export
   
   :timestamp
   :init-timestamp
   :make-timestamp
   :timestamp-vclock
   :promote-timestamp
   :compare-and-update
   :concurrent-access
   :compare-vector-lengths
   :timestamp<=))

(defpackage :cl-replica.network-io
  (:use :cl)
  (:import-from :cl-replica.vector-clock :init-timestamp)
  (:export
   
   ;; functions
   :stop-communication
   :start-server
   :connect-to-remote-peer
   :send-update
   :make-network-settings
   :network-settings-max-n-of-tcp-connections
   :network-settings-htable-entry-size
   :network-settings-header-bytes
   :network-settings-trailing-bytes
   :network-settings-server-buffer-size
   :network-settings-client-buffer-size
   :network-settings-udp-socket
   :network-settings-stop-udp-server
   :network-settings-stop-tcp-server
   :network-settings-stop-tcp-client
   :network-settings-tcp-client-try-reconnect-after
   :network-settings-tcp-client-reconnection-attempts
   :network-settings-stop-sync
   :network-settings-time-to-wait-if-no-data
   :network-settings-encryption-fns
   :network-settings-share-cache-interval-in-sec   
   :network-settings-remove-obsolete-keys-interval
   :network-settings-stop-hash-table-cleaning
   :network-settings-cache-being-processed
   :send-echo))   

(defpackage :cl-replica.hashtable-ops
  (:use :cl :cl-replica.vector-clock :cl-replica.network-io)
  (:export
   
   :make-shared-hash-table
   :shared-hash-table-lock
   :shared-hash-table-number-of-nodes
   :shared-hash-table-other-nodes
   :shared-hash-table-this-node-idx
   :shared-hash-table-this-node
   :shared-hash-table-other-nodes
   :shared-hash-table-table
   :shared-hash-table-last-keys-modified
   :shared-hash-table-last-keys-modified-max-length
   :shared-hash-table-server-input-queue
   :shared-hash-table-clients-socket-pool
   :shared-hash-table-server-socket
   :shared-hash-table-destroyed-p

   :gvalue-dbind
   :init-gvalue
   :sort-for-consistency
   :remhash-shared-no-lock
   :gethash-shared-no-lock
   :newhash-shared-no-lock
   :apply-updates-from-other-nodes
   :share-cache-periodically
   :remove-obsolete-keys
   ;; conditions
   :cache-being-shared))

(defpackage :cl-replica
  (:use :cl :cl-replica.hashtable-ops :cl-replica.vector-clock :cl-replica.network-io)
  (:export
   
   :share-hash-table
   :gethash-shared
   :newhash-shared
   :remhash-shared
   :maphash-shared
   :clrhash-shared   
   :destroy-shared-htable))
   
   
