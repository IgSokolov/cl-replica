(defpackage :cl-replica.u-tests
  (:use :cl :cl-replica :cl-replica.vector-clock :cl-replica.hashtable-ops :cl-replica.network-io))

(in-package :cl-replica.u-tests)

(eval-when (:compile-toplevel :load-toplevel :execute)
  (declaim (optimize (debug 3))))
  
(defmacro ignorable-let (bindings &body body)
  "Creates ignorable bindings"
  `(let ,bindings
     (declare (ignorable ,@(mapcar #'car bindings)))
     ,@body))

(defmacro inspect-h-table ((h-table-obj &optional (wait-after-socket-is-closed 0)) &body body)
  "Bind slots to variables, execute the body and destroy h-table-obj."
  (let ((sht (gensym))
  	(settings (gensym)))
    `(destructuring-bind (,sht ,settings) ,h-table-obj
       (ignorable-let ((node (shared-hash-table-this-node ,sht)) ;; shared-hash-table slots
		       (number-of-nodes (shared-hash-table-number-of-nodes ,sht))
		       (other-nodes (shared-hash-table-other-nodes ,sht))
		       (idx (shared-hash-table-this-node-idx ,sht))
		       (table (shared-hash-table-table ,sht))
		       (queue (shared-hash-table-server-input-queue ,sht))
		       (cache (shared-hash-table-last-keys-modified ,sht))
		       (cache-length (shared-hash-table-last-keys-modified-max-length ,sht))
		       (server-socket (shared-hash-table-server-socket ,sht))
		       (socket-pool (shared-hash-table-clients-socket-pool ,sht))
		       ;; network-settings slots
		       (entry-size (network-settings-htable-entry-size ,settings))
		       (header-bytes (network-settings-header-bytes ,settings))
		       (trailing-bytes (network-settings-trailing-bytes ,settings))
		       (max-n-of-tcp-connections (network-settings-max-n-of-tcp-connections ,settings))
		       (server-buffer-size (network-settings-server-buffer-size ,settings))
		       (client-buffer-size (network-settings-client-buffer-size ,settings))
		       (udp-socket (network-settings-udp-socket ,settings))
		       (stop-sync (network-settings-stop-sync ,settings))
		       (time-to-wait (network-settings-time-to-wait-if-no-data ,settings))
		       (stop-udp-server (network-settings-stop-udp-server ,settings))
		       (stop-tcp-server (network-settings-stop-tcp-server ,settings))
		       (stop-tcp-client (network-settings-stop-tcp-client ,settings))
		       (recon-after (network-settings-tcp-client-try-reconnect-after ,settings))
		       (recon-attempts (network-settings-tcp-client-reconnection-attempts ,settings))
		       (share-cache-dt (network-settings-share-cache-interval-in-sec ,settings))
		       (cache-being-shared (network-settings-cache-being-processed ,settings))
		       (clean-db-dt (network-settings-remove-obsolete-keys-interval ,settings))
		       (encryption-fns (network-settings-encryption-fns ,settings))
		       (stop-cleaning-db (network-settings-stop-hash-table-cleaning ,settings)))
	 (unwind-protect
	      (progn	     
		,@body)	   
	   (destroy-shared-htable ,h-table-obj ,wait-after-socket-is-closed))))))

(defun mutex-is-free-p (sht)
  (not (sb-thread:mutex-value (shared-hash-table-lock sht))))


(defun make-h-table-offline (h-table-obj)
  (cl-replica::dbind-with-mutex (sht settings) h-table-obj
    (cl-replica::stop-all-network-threads settings)))
  
;; Network IO tests

(defun newhash-shared-test (addr-1 addr-2)
  "Check sharing a value when it is added locally"
  (format t "Running newhash-shared-test~%")
  (let ((h1 (make-hash-table))
	(h2 (make-hash-table)))
    (let ((h-table-obj-1 (share-hash-table h1 :this-node addr-1
					      :other-nodes (list addr-2)
					      :cache-length 1 ;; = no cache
					      :share-cache-interval-in-sec 5))
	  (h-table-obj-2 (share-hash-table h2 :this-node addr-2
					      :other-nodes (list addr-1)
					      :cache-length 1 ;; = no cache
					      :share-cache-interval-in-sec 5)))
      (newhash-shared 0 h-table-obj-1 "00") ;; will not be in cache and must be shared right now
      (newhash-shared 1 h-table-obj-1 "01")
      (newhash-shared 2 h-table-obj-1 "02")
      (sleep 1)
      (inspect-h-table (h-table-obj-1)
	(inspect-h-table (h-table-obj-2 1)
	  ;; (maphash #'(lambda (key value) (format t "~a->~a~%" key value)) table))))))    
	  (assert (string= "00" (gethash-shared 0 h-table-obj-2))))))))

(defun remhash-shared-test (addr-1 addr-2)
  "Check sharing a value when it is deleted locally"
  (format t "Running remhash-shared-test~%")
  (let ((h1 (make-hash-table))
	(h2 (make-hash-table)))
    (let ((h-table-obj-1 (share-hash-table h1 :this-node addr-1
					      :other-nodes (list addr-2)
					      :cache-length 3					      
					      ;; share death-certificate faster than ..
					      :share-cache-interval-in-sec 1
					      ;; .. the value is removed 
					      :remove-obsolete-keys-interval 10)) 
	  (h-table-obj-2 (share-hash-table h2 :this-node addr-2
					      :other-nodes (list addr-1)
					      :cache-length 3					      
					      :share-cache-interval-in-sec 1
					      :remove-obsolete-keys-interval 10)))
      (newhash-shared 0 h-table-obj-1 "00")
      (sleep 1) ;; wait for send-update
      (inspect-h-table (h-table-obj-1)
	(inspect-h-table (h-table-obj-2 1)	  
	  (assert (string= "00" (gethash-shared 0 h-table-obj-2))) ;; data were delivered	  
	  (remhash-shared 0 h-table-obj-1) ;; delete locally	  
	  (assert (not (gethash-shared 0 h-table-obj-1))) ;; key was really deleted	  
	  (assert (gethash-shared 0 h-table-obj-2)))))))  ;; key was deleted on the remote peer

(defun remove-obsolete-keys-test (addr-1 addr-2)
  "Check that keys with death-certificate are deleted after :remove-obsolete-keys-interval"
  (format t "Running remove-obsolete-keys-test~%")
  (let ((h1 (make-hash-table))
	(h2 (make-hash-table)))
    (let ((h-table-obj-1 (share-hash-table h1 :this-node addr-1
					      :other-nodes (list addr-2)
					      :cache-length 3					      
					      :share-cache-interval-in-sec 10
					      :remove-obsolete-keys-interval 1))
	  (h-table-obj-2 (share-hash-table h2 :this-node addr-2
					      :other-nodes (list addr-1)
					      :cache-length 3					      
					      :share-cache-interval-in-sec 10
					      :remove-obsolete-keys-interval 1)))
      (newhash-shared 0 h-table-obj-1 "00")      
      (remhash-shared 0 h-table-obj-1)
      (sleep 1) ;; wait for cleaning service      
      (inspect-h-table (h-table-obj-2)
	(inspect-h-table (h-table-obj-1 1)
	  (assert (not (gethash 0 table)))))))) ;; data were really deleted

(defun cache-sharing-test (addr-1 addr-2)
  "Check cache sharing across addr-1 addr-2"
  (format t "Running cache-sharing-test~%")
  (let ((h1 (make-hash-table))
	(h2 (make-hash-table)))
    ;; These values are added before shared-hash-tables are created.
    ;; Hence they can be transferred only by cache-sharing.
    (setf (gethash 0 h1) "00")
    (setf (gethash 1 h1) "01")
    (setf (gethash 2 h1) "02")
    (let ((h-table-obj-1 (share-hash-table h1 :this-node addr-1
					      :other-nodes (list addr-2)
					      :cache-length 100					      
					      :share-cache-interval-in-sec 0.1))				 
	  (h-table-obj-2 (share-hash-table h2 :this-node addr-2
					      :other-nodes (list addr-1)
					      :cache-length 100
					      :share-cache-interval-in-sec 0.1)))
      (sleep 1.5) ;; wait until cache-sharing does the job      
      (inspect-h-table (h-table-obj-1)
	(inspect-h-table (h-table-obj-2)
	  ;; (maphash #'(lambda (key value) (format t "~a->~a~%" key value)) table)	  	  
	  (assert (string= "00" (gethash-shared 0 h-table-obj-2)))
	  (assert (string= "01" (gethash-shared 1 h-table-obj-2)))
	  (assert (string= "02" (gethash-shared 2 h-table-obj-2))))))))

(defun clrhash-shared-test (addr-1)
  "Ensure one can empty shared-hash-table"
  (format t "Running clrhash-shared-test~%")
  (let ((h1 (make-hash-table)))
    (setf (gethash 0 h1) "00")
    (setf (gethash 1 h1) "01")
    (setf (gethash 2 h1) "02")
    (let ((h-table-obj-1 (share-hash-table h1 :this-node addr-1
					      :other-nodes (list addr-1))))
      ;; make the hash-table offline
      (make-h-table-offline h-table-obj-1)
      ;; try with dbind-with-mutex
      ;; ensure that the values present
      (assert (string= "00" (gethash-shared 0 h-table-obj-1)))
      (assert (string= "01" (gethash-shared 1 h-table-obj-1)))
      (assert (string= "02" (gethash-shared 2 h-table-obj-1)))
      ;; delete all keys
      (clrhash-shared h-table-obj-1)
      (inspect-h-table (h-table-obj-1)
	(assert
	 (and
	  (not (gethash-shared 0 h-table-obj-1))
	  (not (gethash-shared 1 h-table-obj-1))
	  (not (gethash-shared 2 h-table-obj-1))))))))
      
(defun maphash-shared-test (addr-1)
  "Check maphash analog for shared-hash-table"
  (format t "Running maphash-shared-test~%")
  (let ((h1 (make-hash-table)))
    (setf (gethash 0 h1) "00")
    (setf (gethash 1 h1) "01")
    (setf (gethash 2 h1) "02")
    (let ((h-table-obj-1 (share-hash-table h1 :this-node addr-1
					      :other-nodes (list addr-1))))
      ;; make the hash-table offline
      (make-h-table-offline h-table-obj-1)
      (remhash-shared 1 h-table-obj-1)
      (inspect-h-table (h-table-obj-1)
	(let ((buffer))
	  (flet ((push-key-value (key value)
		   (push (list key value) buffer)))
	    (maphash-shared #'(lambda (key value) (push-key-value key value)) h-table-obj-1)
	    ;; check that #'push-key-value did the job
	    (flet ((assert-key-value (key value)
		 (ccase key
		   (0 (assert (string= value "00")))
		   (1 (error "key must not be there"))
		   (2 (assert (string= value "02"))))))
	      (mapcar #'(lambda(item) (assert-key-value (first item) (second item))) buffer))))))))
	      
(defun block-api-while-cache-is-shared-test (addr-1)
  "Ensure API is blocked when cache is being shared"
  (format t "Running block-api-while-cache-is-shared-test~%")
  (let ((h1 (make-hash-table)))
    (setf (gethash 0 h1) "00")
    (let ((h-table-obj-1 (share-hash-table h1 :this-node addr-1
					      :other-nodes (list addr-1))))
      ;; make the hash-table offline
      (make-h-table-offline h-table-obj-1)
      (setf (network-settings-cache-being-processed (second h-table-obj-1)) t) ;; API is now blocked
      (inspect-h-table (h-table-obj-1)
	(handler-case
	    (progn
	      (gethash-shared 0 h-table-obj-1) ;; must throw condition
	      (assert NIL))                    ;; must not be executed	  
	  (CACHE-BEING-SHARED (c)
	    (declare (ignore c))))))))

(defun destroy-destroyed-shared-htable (addr-1)
  "Ensure safe call of #'destroy-shared-htable on a destroyed shared-htable"
  (format t "Running destroy-destroyed-shared-htable-test~%")
  (let ((h1 (make-hash-table)))
    (let ((h-table-obj-1 (share-hash-table h1 :this-node addr-1
					      :other-nodes (list addr-1)
					      :cache-length 3					      
					      :share-cache-interval-in-sec 5
					      :remove-obsolete-keys-interval 1)))
      (destroy-shared-htable h-table-obj-1 0)
      (destroy-shared-htable h-table-obj-1 0))))
      
(defun performance-test (addr-1 addr-2 n)
  "Checks whether n messages are tranferred from addr-1 to addr-2"
  (format t "Running performance-test~%")
  (flet ((get-random-buffer-size (min max)
	     (+ min (random max))))
    (let ((h1 (make-hash-table))
	  (h2 (make-hash-table))
	  (min 200000)
	  (max 500000))      
      (let ((h-table-obj-1 (share-hash-table h1 :this-node addr-1
						:other-nodes (list addr-2)
						:cache-length 10
						:htable-entry-size 100
						:server-buffer-size (get-random-buffer-size min max)
						:client-buffer-size NIL ;; strongly recommended
						:tcp-client-reconnection-attempts 10
						:tcp-client-try-reconnect-after 1
						:share-cache-interval-in-sec 60
						:remove-obsolete-keys-interval 60))					      								 
	    (h-table-obj-2 (share-hash-table h2 :this-node addr-2
						:other-nodes (list addr-1)
						:htable-entry-size 100
						:cache-length 10
						:server-buffer-size (get-random-buffer-size min max)
						:client-buffer-size NIL
						:tcp-client-reconnection-attempts 10
						:tcp-client-try-reconnect-after 1
						:share-cache-interval-in-sec 60
						:remove-obsolete-keys-interval 60)))	
	(loop for key from 0 below n do
	  ;; (sleep 0.01) ;; prevents socket buffer overflow if UDP sockets are tested
	  (newhash-shared key h-table-obj-1 (write-to-string key))) ;; ex.: key = 5, value = "5"
	(sleep 1)
	(destroy-shared-htable h-table-obj-1 0)
	(inspect-h-table (h-table-obj-2)
	  (assert (= n (hash-table-count (destroy-shared-htable h-table-obj-2 3)))))))))

(defun encrypt-traffic-test (addr-1 addr-2)
  "Check encryption of network traffic"
  (format t "Running encrypt-traffic-test~%")
  (let ((h1 (make-hash-table))
	(h2 (make-hash-table)))
    (flet ((test-encrypt (byte-array)
	     (nreverse byte-array))) ;; data = (test-encrypt (test-encrypt data))
      (let ((h-table-obj-1 (share-hash-table h1 :this-node addr-1
						:other-nodes (list addr-2)
						:encryption-fns (list #'test-encrypt #'test-encrypt)))
	    (h-table-obj-2 (share-hash-table h2 :this-node addr-2
						:other-nodes (list addr-1)
						:encryption-fns (list #'test-encrypt #'test-encrypt))))
	(newhash-shared 123456789 h-table-obj-1 "123456789")
	(sleep 1)
	(inspect-h-table (h-table-obj-1)
	  (inspect-h-table (h-table-obj-2)	    
	    (assert (string= "123456789" (gethash-shared 123456789 h-table-obj-2)))))))))


(defun multinode-slow-cache-test (addr-1 addr-2 addr-3 addr-4)
  "Check correct data sharing when new data are added lcally.
   Test the following node sharing pattern: 1 -> (2,3,4); 2 -> (1,3); 3 -> 4; 4 -> 3"
  (format t "Running multinode-slow-cache-test~%")
  (let ((h1 (make-hash-table))
	(h2 (make-hash-table))
	(h3 (make-hash-table))
	(h4 (make-hash-table)))	 
    (let ((h-table-obj-1 (share-hash-table h1 :this-node addr-1
					      :other-nodes (list addr-2 addr-3 addr-4)
					      ;; ensures that values are shared just after they were added locally
					      :share-cache-interval-in-sec 60))
	  (h-table-obj-2 (share-hash-table h2 :this-node addr-2
					      :other-nodes (list addr-1 addr-3)
					      :share-cache-interval-in-sec 60))
	  (h-table-obj-3 (share-hash-table h3 :this-node addr-3
					      :other-nodes (list addr-4)
					      :share-cache-interval-in-sec 60))
	  (h-table-obj-4 (share-hash-table h4 :this-node addr-4
					      :other-nodes (list addr-3)
					      :share-cache-interval-in-sec 60)))
      (newhash-shared 1 h-table-obj-1 "1") ;; shared with nodes 2,3,4
      (sleep 1)
      (newhash-shared 2 h-table-obj-2 "2") ;; shared with nodes 1,3
      (sleep 1)
      (newhash-shared 3 h-table-obj-3 "3") ;; shared with node 4
      (sleep 1)
      (newhash-shared 4 h-table-obj-4 "4") ;; shared with node 3
      (sleep 1)
      (inspect-h-table (h-table-obj-1 1)
	(assert (string= "1" (gethash-shared 1 h-table-obj-1))) ;; local value
	(assert (string= "2" (gethash-shared 2 h-table-obj-1))) ;; recv from node 2
	(assert (not (gethash-shared 3 h-table-obj-1)))
	(assert (not (gethash-shared 4 h-table-obj-1)))	
	(inspect-h-table (h-table-obj-2 1)
	  (assert (string= "2" (gethash-shared 2 h-table-obj-2))) ;; local value
	  (assert (string= "1" (gethash-shared 1 h-table-obj-2))) ;; recv from node 1
	  (assert (not (gethash-shared 3 h-table-obj-2)))
	  (assert (not (gethash-shared 4 h-table-obj-2)))	
	  (inspect-h-table (h-table-obj-3 1)
	    (assert (string= "3" (gethash-shared 3 h-table-obj-3))) ;; local value							 
	    (assert (string= "1" (gethash-shared 1 h-table-obj-3))) ;; recv from node 1
	    (assert (string= "2" (gethash-shared 2 h-table-obj-3))) ;; recv from node 2
	    (assert (string= "4" (gethash-shared 4 h-table-obj-3))) ;; recv from node 4
	    (inspect-h-table (h-table-obj-4 1)
	      (assert (string= "4" (gethash-shared 4 h-table-obj-4))) ;; local value
	      (assert (string= "1" (gethash-shared 1 h-table-obj-4))) ;; recv from node 1								 
	      (assert (string= "3" (gethash-shared 3 h-table-obj-4))) ;; recv from node 3  
	      (assert (not (gethash-shared 2 h-table-obj-4))))))))))

(defun multinode-fast-cache-test (addr-1 addr-2 addr-3 addr-4)
  "Check correct data sharing via cache.
   This process is the heart of the epidemic data dissemination.
   Test the following sharing pattern: 1 -> 2; 2 -> 3; 3 -> 4; 4 -> 4"
  (format t "Running multinode-fast-cache-test~%")
  (let ((h1 (make-hash-table))
	(h2 (make-hash-table))
	(h3 (make-hash-table))
	(h4 (make-hash-table)))	 
    (let ((h-table-obj-1 (share-hash-table h1 :this-node addr-1
					      :other-nodes (list addr-2)
					      :share-cache-interval-in-sec 0.1))
	  (h-table-obj-2 (share-hash-table h2 :this-node addr-2
					      :other-nodes (list addr-3)
					      :share-cache-interval-in-sec 0.1))
	  (h-table-obj-3 (share-hash-table h3 :this-node addr-3
					      :other-nodes (list addr-4)
					      :share-cache-interval-in-sec 0.1))
	  (h-table-obj-4 (share-hash-table h4 :this-node addr-4
					      :other-nodes (list addr-1)
					      :share-cache-interval-in-sec 0.1)))
      (newhash-shared 1 h-table-obj-1 "1") ;; must be shared with all nodes via cache (1 -> 2; 2 -> 3; 3 -> 4)
      (sleep 4)      
      (inspect-h-table (h-table-obj-1)	
	(inspect-h-table (h-table-obj-2)	  
	  (inspect-h-table (h-table-obj-3)
	    (inspect-h-table (h-table-obj-4)
	      (assert (string= "1" (gethash-shared 1 h-table-obj-1))) ;; local value
	      (assert (string= "1" (gethash-shared 1 h-table-obj-2))) ;; recv from node 1 directly
	      (assert (string= "1" (gethash-shared 1 h-table-obj-3))) ;; recv from node 1 indirectly
	      (assert (string= "1" (gethash-shared 1 h-table-obj-4)))))))))) ;; recv from node 1 indirectly

;; Vector clocks
  
(defun init-ts-test ()
  (format t "Running init-ts-test~%")
  (let ((ts (init-timestamp 2)))
    (assert (not (mismatch (timestamp-vclock ts) #(0 0))))
    ts))

(defun promote-ts-test (ts)
  (format t "Running promote-ts-test~%")
  (promote-timestamp ts 1)
  (assert (not (mismatch (timestamp-vclock ts) #(0 1))))
  ts)

(defun compare-and-update-test (ts)
  (format t "Running compare-and-update-test~%")
  (promote-timestamp ts 0)
  (promote-timestamp ts 0)
  (let ((ts-new (init-timestamp 2)))
    (promote-timestamp ts-new 1)
    (compare-and-update ts ts-new)
    (assert (not (mismatch (timestamp-vclock ts) #(2 1))))))

(defun timestamp<=-test ()
  (format t "Running timestamp<=-test~%")
  (let ((ts-base (make-timestamp :vclock #(1 2 3)))
	(ts-1 (make-timestamp :vclock #(1 2 3)))  ;; equal
	(ts-2 (make-timestamp :vclock #(2 3 4)))  ;; ts-2 > ts-base
	(ts-3 (make-timestamp :vclock #(0 1 2)))  ;; ts-3 < ts-base
	(ts-4 (make-timestamp :vclock #(0 3 2)))) ;; data race
    (assert (eq t (timestamp<= ts-base ts-1)))
    (assert (eq t (timestamp<= ts-base ts-2)))
    (assert (eq NIL (timestamp<= ts-base ts-3)))
    (assert (eq NIL (timestamp<= ts-base ts-4)))))

(defun run-networking-tests (addr-1 addr-2 addr-3 addr-4)
  (newhash-shared-test addr-1 addr-2)
  (remhash-shared-test addr-1 addr-2)
  (remove-obsolete-keys-test addr-1 addr-2)
  (cache-sharing-test addr-1 addr-2)
  (clrhash-shared-test addr-1)
  (maphash-shared-test addr-1)
  (block-api-while-cache-is-shared-test addr-1)
  (destroy-destroyed-shared-htable addr-1)
  (performance-test addr-1 addr-2 10)
  (encrypt-traffic-test addr-1 addr-2)
  (multinode-slow-cache-test addr-1 addr-2 addr-3 addr-4)
  (multinode-fast-cache-test addr-1 addr-2 addr-3 addr-4))

;; add parser test

(defun run-vector-clock-tests ()
  (promote-ts-test (init-ts-test))
  (compare-and-update-test (init-ts-test))
  (timestamp<=-test)) 

(defun run-unit-tests ()
  (run-vector-clock-tests)
  (run-networking-tests  "tcp://127.0.0.1:5501" "tcp://127.0.0.1:5502" "tcp://127.0.0.1:5503" "tcp://127.0.0.1:5504"))
