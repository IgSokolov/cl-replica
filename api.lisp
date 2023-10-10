(in-package :cl-replica)

(eval-when (:compile-toplevel :load-toplevel :execute)
  (declaim (optimize (debug 3)))
  (require :sb-concurrency))

#|
Explanation of the share-hash-table parameter list:
1. Address of hash-table replicas
   * this-node: socket-type, ip and port of the local machine (ex.: "tcp://127.0.0.1:5000")
   * other-nodes: list of remote peers (ex.: (list "tcp://192.168.1.1:5000" "tcp://192.168.1.2:5000")
2. TCP message boundary markers
   * message-frame-header: header marker
   * message-frame-trail: termination marker
   Their default values are "start" and "end", but you can choose any strings. Better not too short. 
3. * htable-entry-size: approximate length of the byte array with an encoded key-value pair.
4. Cache:
   * cache-length: number of elements in the last-keys-modified list. This list is periodically shared.
   * share-cache-interval-in-sec: controls how often this list is shared with remote peers.   
5. Socket settings:
   * server-buffer-size: buffer size of socket-receive method
   * client-buffer-size: buffer size of socket-send method. NIL = length of transferred data is used
   * max-n-of-tcp-connections, tcp-client-try-reconnect-after, tcp-client-reconnection-attempts: obvious settings
6. * time-to-wait-if-no-data: don't process internal data queue if network activity is low and no data are enqueued.
7. Hash-table cleaning:
   * remove-obsolete-keys-interval: (time in sec) controls cleaning the hash-table from the keys which were deleted with #'remhash-shared.
                                    It is important to let the obsolete keys be distributed over network and then delete them collectively.
                                    Otherwise the deleted item will be pushed back by the network environment. The bigger the network,
                                    the longer this time interval should be. 
9. * encryption-fns: (list #encryption-function #decryption-function). Encrypts network traffic.
|#
(defun share-hash-table (h-table &key this-node other-nodes (message-frame-header "start")
				   (message-frame-trail "end") (htable-entry-size 100)
				   (cache-length 1024) (server-buffer-size 2500000)
				   (client-buffer-size NIL)
				   (time-to-wait-if-no-data 0.1) (max-n-of-tcp-connections 100)
				   (tcp-client-try-reconnect-after 1) (tcp-client-reconnection-attempts 1000)
				   (share-cache-interval-in-sec 1) (remove-obsolete-keys-interval 60)
				   (encryption-fns NIL))			   
  "Nondestructively converts hash-table <key,value> into:
      1. hash-table <key, #(value, death-certificate, timestamp)>
      2. cache (list key timestamp)
   and returns shared-hash-table-object."
  (let* ((list-of-nodes (sort-for-consistency (copy-list (cons this-node other-nodes))))
	 (this-node-idx (position this-node list-of-nodes :test #'string=))
	 (number-of-nodes (list-length list-of-nodes))
	 (last-keys-modified)
	 (h (make-hash-table ;; copy h-table properties
            :test (hash-table-test h-table)
            :rehash-size (hash-table-rehash-size h-table)
            :rehash-threshold (hash-table-rehash-threshold h-table)
            :size (hash-table-size h-table))))
    ;; fill the h-table with gvalues.
    (maphash #'(lambda (key value)
		 (progn
		   (setf (gethash key h)
			 (init-gvalue value NIL (init-timestamp number-of-nodes)))
		   (push (cons key (init-timestamp number-of-nodes)) last-keys-modified)))		 
	     h-table)
    ;; set environment with settings which affect shared-hash-table
    ;; and which can be changed in run-time; then create shared-hash-table.
    (let ((server-input-queue (sb-concurrency:make-queue :name 'server-queue :initial-contents NIL))
	  (settings (make-network-settings
		     :htable-entry-size htable-entry-size
		     :max-n-of-tcp-connections max-n-of-tcp-connections
		     :header-bytes (string-to-octets message-frame-header :external-format :utf-8)
		     :trailing-bytes (string-to-octets message-frame-trail :external-format :utf-8)
		     :server-buffer-size server-buffer-size
		     :client-buffer-size client-buffer-size					   
		     :stop-sync NIL
		     :time-to-wait-if-no-data time-to-wait-if-no-data
		     :stop-udp-server NIL
		     :stop-tcp-server NIL
		     :stop-tcp-client NIL
		     :tcp-client-try-reconnect-after tcp-client-try-reconnect-after
		     :tcp-client-reconnection-attempts tcp-client-reconnection-attempts
		     :share-cache-interval-in-sec share-cache-interval-in-sec
		     :remove-obsolete-keys-interval remove-obsolete-keys-interval
		     :stop-hash-table-cleaning NIL
		     ;; cache-being-processed: flag, which blocks the API while
		     ;; cache is being sharing.
		     :cache-being-processed NIL 
		     :encryption-fns encryption-fns)))
      (let ((sht (make-shared-hash-table
		  :table h
		  :this-node this-node
		  :other-nodes other-nodes
		  :number-of-nodes number-of-nodes
		  :clients-socket-pool (mapcar #'(lambda (node) (connect-to-remote-peer node settings)) other-nodes)
		  :server-input-queue server-input-queue
		  :server-socket (start-server this-node server-input-queue settings)		
		  :this-node-idx this-node-idx
		  :last-keys-modified-max-length cache-length
		  :last-keys-modified last-keys-modified)))
	;; activate synchronization with other nodes
	(apply-updates-from-other-nodes sht settings server-input-queue)
	;; start cache provider
	(share-cache-periodically sht settings)
	;; start hash-table cleaner
	(remove-obsolete-keys sht settings)	
	;; sht (shared-hast-table) and settings are bound
	(sleep 1) ;; let network threads spin up
	(list sht settings))))) ;; = h-table-obj

#|
Implementation note:
Instead of coupling a shared-hash-table with its settings via a separate struct, it is also
possible to use dynamically scoped slots. See the corresponding stackoverflow thread:
https://stackoverflow.com/questions/71082880/common-lisp-structures-with-dynamically-scoped-slots/71085062#71085062
|#

(defmacro dbind-with-mutex (vars h-table-obj &body body)
  "D-bind variables in vars to h-table-obj structure, acquire the lock,
   do the job in @body and release the lock"
  (destructuring-bind (sht settings) vars
    `(destructuring-bind (,sht ,settings) ,h-table-obj
       (declare (ignorable ,sht ,settings))
       (if (network-settings-cache-being-processed ,settings)
	   (error 'cache-being-shared) ;; application must provide error handlers
	   (sb-thread:with-mutex ((shared-hash-table-lock ,sht))
	     (progn ,@body))))))

(defun gethash-shared (key h-table-obj &optional default)
  "API for getting value"
  (dbind-with-mutex (sht settings) h-table-obj
    (multiple-value-bind (value found-p) (gethash-shared-no-lock key sht)
      (if found-p
	  (values value t)
	  (values default NIL)))))

(defun newhash-shared (key h-table-obj new-value)
  "API for setting value"
  (dbind-with-mutex (sht settings) h-table-obj
    (let ((ts (init-timestamp (shared-hash-table-number-of-nodes sht)))) 
      (newhash-shared-no-lock key new-value NIL ts sht settings))))

(defsetf gethash-shared newhash-shared)

(defun remhash-shared (key h-table-obj)
  "API for deleting key"
  (dbind-with-mutex (sht settings) h-table-obj
    (remhash-shared-no-lock key sht)))

(defun clrhash-shared (h-table-obj)
  "API for deleting all keys from h-table."
  (dbind-with-mutex (sht settings) h-table-obj
      (maphash #'(lambda (key value)
		   (declare (ignore value))
		   (remhash-shared-no-lock key sht))
	       (shared-hash-table-table sht)))
    h-table-obj)

(defun maphash-shared (fn h-table-obj)
  "API for iteration over key-value pairs"
  (dbind-with-mutex (sht settings) h-table-obj
    (maphash #'(lambda (key value) (unless (eq t (aref value 1))
				     (apply fn (list key (aref value 0)))))
	       (shared-hash-table-table sht))))

(defun stop-all-network-threads (settings)
  "API for disabling inter-nodes communication"
    (stop-communication :tcp settings :all)
    (stop-communication :udp settings))
    
(defun start-all-network-threads (h-table-obj)
  "API for activating inter nodes communication"
  (destructuring-bind (sht settings) h-table-obj
    ;; set network flags
    (setf (network-settings-stop-sync settings) NIL)
    (setf (network-settings-stop-udp-server settings) NIL)
    (setf (network-settings-stop-tcp-server settings) NIL)
    (setf (network-settings-stop-tcp-client settings) NIL)
    ;; restart network threads
    (setf (shared-hash-table-clients-socket-pool sht)
	  (mapcar #'(lambda (node) (connect-to-remote-peer node settings)) (shared-hash-table-other-nodes sht)))
    (setf (shared-hash-table-server-socket sht) (start-server (shared-hash-table-this-node sht) (shared-hash-table-server-input-queue sht) settings))
    (apply-updates-from-other-nodes sht settings (shared-hash-table-server-input-queue sht))
    (share-cache-periodically sht settings)))
    
(defun destroy-shared-htable (h-table-obj wait-after-socket-is-closed)
  "API to stop all threads inside the shared hash table.
   A simple hash-table is returned."
  (destructuring-bind (sht settings) h-table-obj
    (unless (shared-hash-table-destroyed-p sht) ;; allows calling this function safely on a destroyed shared-h-table
      (setf (shared-hash-table-destroyed-p sht) t)
      (setf (network-settings-stop-hash-table-cleaning settings) t) ;; stop db cleaning
      (stop-all-network-threads settings)
      (send-echo (shared-hash-table-this-node sht) settings) ;; let tcp-server close the socket
      (sleep wait-after-socket-is-closed) ;; let data in queue be added to hash-table
      (setf (network-settings-stop-sync settings) t) ;; stop processing internal queue
      (shared-hash-table-table sht)))) ;; return normal hash-table


