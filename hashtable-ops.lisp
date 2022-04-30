(in-package :cl-replica.hashtable-ops)

(eval-when (:compile-toplevel :load-toplevel :execute)
  (declaim (optimize (debug 3))))

(defstruct shared-hash-table
  (lock (sb-thread:make-mutex))
  this-node
  other-nodes
  number-of-nodes
  this-node-idx
  clients-socket-pool
  server-socket
  server-input-queue
  table
  last-keys-modified
  last-keys-modified-max-length ;; max number of elements in last-key-modified list
  (destroyed-p NIL))

; util
(defun sort-for-consistency (list-of-ips)
  "Sort the list of ips."
  (sort list-of-ips #'string>))

(defun init-gvalue (value del-p timestamp) ;; gvalue = generic-value
  "Constructs a hash-table shared value from normal value"
  (make-array 3 :initial-contents
	      `(,value ,del-p ,timestamp)))				

(defmacro gvalue-dbind (vars gvalue &body body)
  "Destructures gvalue and binds its component to vars"
  `(let ((,(first vars) (aref ,gvalue 0))
	 (,(second vars) (aref ,gvalue 1))
	 (,(third vars) (aref ,gvalue 2)))	 
     (declare (ignorable ,(first vars) ,(second vars) ,(third vars)))   
     (progn ,@body)))

(defun gethash-shared-no-lock (key sht)
  "Retrieves value from h-table based on key"
  (multiple-value-bind (gvalue found-p) (gethash key (shared-hash-table-table sht))
    (if found-p
	(gvalue-dbind (value del-p timestamp) gvalue	  
	  (if del-p
	      (values NIL NIL)
	      (values value t)))
	(values NIL NIL))))

(defun newhash-shared-no-lock (key value del-p timestamp sht settings &optional (infect-p t))
  "Adds data to h-table and updates its cache"
  (when infect-p ;; used to avoid network echo while applying updates fron other nodes
    (mapc #'(lambda (node) (send-update key value del-p timestamp node settings)) (shared-hash-table-clients-socket-pool sht)))
  (multiple-value-bind (own-gvalue found-p) (gethash key (shared-hash-table-table sht))
    (if found-p		      		    
	(gvalue-dbind (own-value own-del-p own-timestamp) own-gvalue	  
	  (when (timestamp<= own-timestamp timestamp)
	    ;; update hash-table
	    (setf (aref (gethash key (shared-hash-table-table sht)) 0) value
		  (aref (gethash key (shared-hash-table-table sht)) 1) del-p)
	    (promote-timestamp (aref (gethash key (shared-hash-table-table sht)) 2) (shared-hash-table-this-node-idx sht))
	    (compare-and-update (aref (gethash key (shared-hash-table-table sht)) 2) timestamp)
	    ;; update last-keys-modified (cache)
	    (let ((pair (find key (shared-hash-table-last-keys-modified sht) :key #'car)))
	      (if pair
		  (let ((current-timestamp (cdr pair)))		    
		    (setf (shared-hash-table-last-keys-modified sht)
			  (remove key (shared-hash-table-last-keys-modified sht) :key #'car))
		    (promote-timestamp current-timestamp (shared-hash-table-this-node-idx sht))
		    (compare-and-update current-timestamp timestamp)
		    (push (cons key current-timestamp) (shared-hash-table-last-keys-modified sht)))
		  (push (cons key (make-timestamp :vclock (timestamp-vclock timestamp))) (shared-hash-table-last-keys-modified sht))))))
	(let ((ts (timestamp-vclock timestamp)))
	  ;; update last-leys-modified
	  (push (cons key (make-timestamp :vclock ts)) (shared-hash-table-last-keys-modified sht))
	  ;; update hash-table
	  (setf (gethash key (shared-hash-table-table sht))
		(init-gvalue value del-p (make-timestamp :vclock ts))))))
  (when (>= (list-length (shared-hash-table-last-keys-modified sht)) (shared-hash-table-last-keys-modified-max-length sht))
    (setf (shared-hash-table-last-keys-modified sht)
	  (butlast (shared-hash-table-last-keys-modified sht))))
  value)

(defun remhash-shared-no-lock (key sht)
  "Deletes key from h-table by setting del-p flag.
   See Demers et al., Epidemic Algorithms for Replicated Database Maintenance (1989)
   to understand why one cannot simply delete a key-value pair from a replicated
   database."
  (multiple-value-bind (gvalue found-p) (gethash key (shared-hash-table-table sht))
    (declare (ignore gvalue))
    ;; we don't touch the cache because when it is shared, the del-p flag will be
    ;; fetched from the hash-table (see #'provide-cache)
    (if found-p
	(progn
	  (setf (aref (gethash key (shared-hash-table-table sht)) 1) t)
	  t)
	NIL)))

(defun clean-shared-hash-table (sht)
  "Deletes all keys which have deatch-certificates (del-p flag)"
  (sb-thread:with-mutex ((shared-hash-table-lock sht))
    (let ((obsolete-keys))
      (maphash #'(lambda (key value) (when (eq t (aref value 1))
				       (push key obsolete-keys)))
	       (shared-hash-table-table sht))
      (dolist (key obsolete-keys)
	(remhash key (shared-hash-table-table sht))))))

(defmacro call-fn-in-interruptable-thread (delay stop-flag n-segments fn-with-args)
  "When delay >> 1, (sleep delay) would block the thread for a long time.
   This macro solves that problem by splitting delay into n-segments time intervals
   and by checking stop-flag n-segments times withing the delay."
  (let ((g-counter (gensym)))
    `(let ((,g-counter 0))
       (setf ,stop-flag NIL)
       (sb-thread:make-thread
	(lambda ()
	  (loop until ,stop-flag do
	    (if (> ,g-counter ,n-segments)
		(progn	     
		  ,fn-with-args
		  (setq ,g-counter 0))
		(progn
		  (sleep (/ ,delay ,n-segments))
		  (incf ,g-counter)))))))))

(defun remove-obsolete-keys (sht settings)
  (call-fn-in-interruptable-thread 
   (network-settings-remove-obsolete-keys-interval settings)
   (network-settings-stop-hash-table-cleaning settings)
   100 ;; check stop-flag (network-settings-stop-hash-table-cleaning) 100 times per delay
   (clean-shared-hash-table sht)))

(define-condition malformed-data (error)
  ((data :initarg :data
         :reader data)
   (msg :initarg :msg
         :reader msg))   
  (:report (lambda (condition stream) (format stream "Malformed data: ~a~%~a~%" (data condition) (msg condition)))))

(defun apply-updates-from-other-nodes (sht settings queue)
  (setf (network-settings-stop-sync settings) NIL)
  (sb-thread:make-thread
   (lambda ()
     ;; read from server buffer
     (loop until (network-settings-stop-sync settings) do
       (let ((data (sb-concurrency:dequeue queue)))
	 (if data
	     (handler-case		 
		 (destructuring-bind (key value del-p timestamp) data
		   (if (typep timestamp 'timestamp)			 
		       (sb-thread:with-mutex ((shared-hash-table-lock sht))
			 (newhash-shared-no-lock key value del-p timestamp sht settings NIL)) ;; NIL = add without sync
		       (error 'malformed-data :data data :msg "Malformed timestamp")))
	       (SB-KERNEL::ARG-COUNT-ERROR (c)
		 (error 'malformed-data :data data :msg c)))		 
	     (sleep (network-settings-time-to-wait-if-no-data settings))))))))
 
;; Daemon for providing cache

(define-condition cache-being-shared (error) ()
  (:documentation "Throwing this error blocks the API and informs a caller that 
                   an application cannot perform any operation on the shared-hash-table.
                   Without this explicit condition, the application would be silently
                   blocked by the mutex inside #'provide-cache. Explicit block is much better
                   because the application has a possibility to use error handlers"))

;; https://rosettacode.org/wiki/Knuth_shuffle#Common_Lisp
(defun nshuffle (sequence)
  (loop for i from (length sequence) downto 2
        do (rotatef (elt sequence (random i))
                    (elt sequence (1- i))))
  sequence)

(defun provide-cache (sht settings)
  "Provides last keys modified in an extended form [(key timestamp) ->
   (list key value death-cert timestamp )]"
  (setf (network-settings-cache-being-processed settings) t)
  (unwind-protect
       (sb-thread:with-mutex ((shared-hash-table-lock sht))
	 ;; update timestamps before sending the list to another node
	 (mapc #'(lambda (pair) (promote-timestamp (cdr pair) (shared-hash-table-this-node-idx sht)))
	       (shared-hash-table-last-keys-modified sht))
	 (let ((cache))
	   ;; sync timestamps in hash-table with last-keys-modified
	   (loop for (key . timestamp) in (shared-hash-table-last-keys-modified sht) do
	     (setf (aref (gethash key (shared-hash-table-table sht)) 2) timestamp)
	     ;; build extended form
	     (let ((gvalue (gethash key (shared-hash-table-table sht))))
	       (gvalue-dbind (value del-p timestamp) gvalue
		 (push (list key value del-p timestamp) cache))))		 
	   cache))
    (setf (network-settings-cache-being-processed settings) NIL)))


;; todo: rewrite (call-fn-in-interruptable-thread such that it can be used here   
(defun share-cache-periodically (sht settings)
  "Provides own cache to a random node"
  (let ((counter 0)
	(dt (network-settings-share-cache-interval-in-sec settings)))
  (setf (network-settings-stop-sync settings) NIL)
  (sb-thread:make-thread
   (lambda ()
     (let ((nodes (nshuffle (copy-list (shared-hash-table-clients-socket-pool sht)))))
       (loop until (network-settings-stop-sync settings) do
	 (loop for node in nodes until (network-settings-stop-sync settings) do
	   (if (> counter 100)
	       (progn
		 ;; build new cache everytime
		 (loop for cache-data in (provide-cache sht settings) do
		   (when cache-data	       
		     (destructuring-bind (key value del-p timestamp) cache-data
		       (send-update key value del-p timestamp node settings))))
		 (setq counter 0))
	       (progn
		 (sleep (/ dt 100))
		 (incf counter))))))))))
