(in-package :cl-replica.network-io)

(eval-when (:compile-toplevel :load-toplevel :execute)
  (declaim (optimize (debug 3)))
  (require :sb-bsd-sockets)
  (require :sb-concurrency))

(defstruct network-settings
  htable-entry-size        ;; approx. size (in bytes) of hash-table entry
  header-bytes             ;; start of message frame
  trailing-bytes           ;; end of message frame
  server-buffer-size
  client-buffer-size       ;; NIL = length of buffer is used  
  udp-socket
  stop-sync                ;; stop inter-nodes communication
  time-to-wait-if-no-data  ;; pause processing internal buffer if no data received
  stop-udp-server          ;; stop 
  stop-tcp-server          ;;      network
  stop-tcp-client          ;;             threads
  max-n-of-tcp-connections ;; 
  tcp-client-try-reconnect-after    ;; delay in secs before tcp client reconnects
  tcp-client-reconnection-attempts  ;; limit reconnection attempts
  share-cache-interval-in-sec       ;; share cache with random node
  remove-obsolete-keys-interval     ;; remove dead key-value pairs form hash-table
  stop-hash-table-cleaning      ;; stops removing obsolete keys in hash-table
  cache-being-processed         ;; if true, API is blocked
  encryption-fns)               ;; (list encrypt-fn decrypt-fn) - cryptographic functionality (optionally provided by an application) to encrypt/decrypt network traffic
  
;;;; address parser

;; https://stackoverflow.com/questions/15393797/lisp-splitting-input-into-separate-strings
(defun split-string-by-delimiter (string delimiterp)
  "Lisp-is-cool-> (Lisp is cool) as strings. Delimiterp is a predicate."
  (loop :for beg = (position-if-not delimiterp string)
     :then (position-if-not delimiterp string :start (1+ end))
     :for end = (and beg (position-if delimiterp string :start beg))
     :when beg :collect (subseq string beg end)
     :while end))

(define-condition network-address-parse-error (error)
  ((address :initarg :address
            :reader address)
   (error-msg :initarg :error-msg
            :reader error-msg))
  (:report (lambda (condition stream) (format stream "Error: ~a~%Cannot parse address: ~a~%Please, use the following template: [protocol]://[ip]:[port] (ex. tcp://192.168.1.1:5550)" (error-msg condition) (address condition)))))

(defun as-keyword (str)
  (values (intern (string-upcase str) "KEYWORD")))

(defun parse-network-address (addr)
  "Parses tcp://192.168.1.1:5550 to (values :tcp #(192 168 1 1) 5550)"
  (flet ((delimiterp (c) (or (char= c #\/) (char= c #\.) (char= c #\:))))        
    (let ((addr-as-list (split-string-by-delimiter addr #'delimiterp)))
      (restart-case
	  (progn
	    (unless
		(= 6 (list-length addr-as-list))	   
	      (error 'network-address-parse-error :address addr))
	    (let ((protocol-str (car addr-as-list))
		  (ip-str (subseq addr-as-list 1 5))
		  (port-str (car (last addr-as-list))))
	      (handler-case
		  (values (as-keyword protocol-str)
			  (make-array 4 :initial-contents (mapcar #'parse-integer ip-str))
			  (parse-integer port-str))
		(SB-INT:SIMPLE-PARSE-ERROR (c)	    
		  (error 'network-address-parse-error :error-msg c :address addr))	    
		(TYPE-ERROR (c)
		  (error 'network-address-parse-error :error-msg c :address addr)))))
	(choose-another-address (new-address)
	  :report "Please choose another address (ex. tcp://127.0.0.1:5550)"
	  :interactive (lambda ()
			 (format t "Enter a new address: ")
			 (list (read)))
	  (parse-network-address new-address))))))
			    
;;;; serialize/deserialize
(defun obj-to-str (obj)
  (with-standard-io-syntax
    (write-to-string obj)))

(define-condition cannot-build-Lisp-object-from-string (error)
  ((data :initarg :data
         :reader data)
   (error-msg :initarg :error-msg
              :reader error-msg))
  (:report (lambda (condition stream) (format stream "Error: ~a~%:Cannot build Lisp object from ~a~%" (error-msg condition) (data condition)))))

(defun str-to-obj (str)
  (with-standard-io-syntax
    (let ((*read-eval* NIL)) ;; evaluating arbitrary Lisp code is dangerous
      (handler-case
	  (read-from-string str)
	(SB-INT:SIMPLE-READER-ERROR (c)
	  (error 'cannot-build-Lisp-object-from-string :error-msg c :data str))
	(END-OF-FILE (c)
	  (error 'cannot-build-Lisp-object-from-string :error-msg c :data str))))))

;; if encryption of network traffic is required, provide the slot network-settings-encryption-fns
(defun encrypt (data encrypt-fn)
  (if encrypt-fn
      (funcall encrypt-fn data)
      data))

(defun decrypt (data decrypt-fn)
  (if decrypt-fn
      (funcall decrypt-fn data)
      data))

;; string encoding/decoding
(defun encode (obj) 
  (string-to-octets (obj-to-str obj) :external-format :utf-8))

(defun decode (vec)  
  (str-to-obj (octets-to-string vec :external-format :utf-8)))

(defun parse-buffer (header-bytes header-size trailing-bytes trailing-size buffer)
  "Recursively traverses buffer and tries hard to find message frames separated by
   starting (header-bytes) and terminating (trailing-bytes) sequencies.
   The algorithm works well even if
       - small buffer size is used
       - buffer contains junk data bytes from previous calls
       - buffer contains ill-positioned message markers
   However, if the buffer size is really tiny, some messages can get lost.
   Note: buffer-size and trailing-size are provided as arguments and not calculated inside
         the function because a caller knows these values."
  (let ((buffer-head) ;; chunk of buffer with trailing-bytes which precedes the kernel with messages
	(frames)      ;; list of "ready-to-decode" data frames (without message markers)
	(pointer 0))  ;; points to some place in buffer
    ;; look for start/stop markers and return early if necessary
    (let ((header-idx (search header-bytes (subseq buffer 0)))
	  (trailing-idx (search trailing-bytes (subseq buffer 0))))
      (cond
	((and (not header-idx) (not trailing-idx))
	 (values NIL NIL NIL))               ;; buffer has no data frames
	((and header-idx (not trailing-idx)) ;; only msg start 
	 (values NIL NIL (subseq buffer header-idx)))
	((and trailing-idx (not header-idx)) ;; only msg end
	 (values (subseq buffer 0 (+ trailing-idx trailing-size)) NIL NIL))
	(t ;; no need to return early. 
	;; process head
	 (if (< header-idx trailing-idx) ;; start and end markers are not NIL and can be compared
	  (setq pointer header-idx)      ;; buffer-head remains NIL
	  (progn
	    (setq buffer-head (subseq buffer 0 (+ trailing-idx trailing-size)))
	    (setq pointer (+ 1 trailing-idx))))
	 ;; define functions to process kernel
	 (labels ((rec-drop-extra-headers-in-frame (pointer header-bytes header-size trailing-idx)
		    ;; recursively traverse the buffer and return a pointer to a byte after the most recent header
		    (let ((header-idx (search header-bytes (subseq buffer pointer (+ pointer trailing-idx)))))
		      (if header-idx
			  (rec-drop-extra-headers-in-frame (+ pointer header-idx header-size) header-bytes header-size (- trailing-idx header-idx header-size))
			  (values pointer trailing-idx)))))
	   (labels ((rec-parse (header-bytes header-size trailing-bytes trailing-size pointer buffer)
		      ;; recursively traverse the buffer and return "ready-to-decode" messages and
		      ;; pointer to a byte after the last message
		      (let ((header-idx (search header-bytes (subseq buffer pointer))))
			(if header-idx
			    (progn		       
			      (setq pointer (+ pointer header-idx header-size)) ;; go to the start of the msg					  			    
			      (let ((trailing-idx (search trailing-bytes (subseq buffer pointer))))
				(if trailing-idx
				    (progn
				      ;; remove junk with ill-positioned headers
				      (multiple-value-bind (new-pointer new-trailing-idx) (rec-drop-extra-headers-in-frame pointer header-bytes header-size trailing-idx)					
					(when (and new-pointer new-trailing-idx)
					  (setq pointer new-pointer)
					  (setq trailing-idx new-trailing-idx)))
				      ;; save the found message frame and go to the next one
				      (push (subseq buffer pointer (+ pointer trailing-idx)) frames)
				      (setq pointer (+ pointer trailing-idx trailing-size)) ;; go to the end of the trailing sequence				    
				      (rec-parse header-bytes header-size trailing-bytes trailing-size pointer buffer))
				    (values frames (- pointer header-size)))))
			    (values frames pointer)))))
	     ;; process kernel
	     (multiple-value-bind (frames pointer) (rec-parse header-bytes header-size trailing-bytes trailing-size pointer buffer)
	       ;; process tail	       
	       (let ((header-idx (search header-bytes (subseq buffer pointer)))
		     (trailing-idx (search trailing-bytes (subseq buffer pointer))))		 
		 (cond
		   ((and (not header-idx) (not trailing-idx))
		    (values buffer-head frames NIL)) ;; no markers in tail
		   ((and header-idx (not trailing-idx)) ;; only msg start		    
		    (values buffer-head frames (subseq buffer (+ pointer header-idx))))
		   ((and trailing-idx (not header-idx)) ;; only msg end
		    (values buffer-head frames NIL))
		   (t
		    (error "parse-buffer fatal error"))))))))))))

;; server side
(define-condition msg-markers-found-in-data (error)
    ((operation :initarg :operation
		:reader operation)
     (marker :initarg :marker
	     :reader marker)
     (idx :initarg :idx
	  :reader idx))
  (:report (lambda (condition stream) (format stream "~a error. ~a marker found in data at index ~a~%" (operation condition) (marker condition) (idx condition)))))
      
(defun ensure-no-msg-makers (msg header-bytes trailing-bytes operation)
  "Throws error if message markers are found in message"
  (let ((header-idx (search header-bytes (subseq msg 0)))
	(trailing-idx (search trailing-bytes (subseq msg 0))))
    (cond
      (header-idx
       (error 'msg-markers-found-in-data :operation operation :marker 'header :idx header-idx))
      (trailing-idx
       (error 'msg-markers-found-in-data :operation operation :marker 'trailing :idx trailing-idx)))))

(defstruct decoder
  acc
  acc-min-size
  header-bytes 
  header-size
  trailing-bytes
  trailing-size
  decrypt-fn
  prev-buffer-trail
  prev-buffer-head)

(defun init-decoder (settings)
  (make-decoder :acc-min-size (/ (network-settings-htable-entry-size settings) 2) ;; should guarantee that at most 1 message can get lost
		:header-bytes (network-settings-header-bytes settings)
		:header-size (length (network-settings-header-bytes settings))
		:trailing-bytes (network-settings-trailing-bytes settings)
		:trailing-size (length (network-settings-trailing-bytes settings))
		:decrypt-fn (second (network-settings-encryption-fns settings))))

(defun decode-buffer (buffer decoder)
  "Implements two-step buffer decoder"
  (let ((data)
	(header-bytes (decoder-header-bytes decoder))
	(header-size (decoder-header-size decoder))
	(trailing-bytes (decoder-trailing-bytes decoder))
	(trailing-size (decoder-trailing-size decoder))
	(decrypt-fn (decoder-decrypt-fn decoder)))	
    ;; decode buffer kernel    
    (multiple-value-bind (buffer-head data-frames buffer-trail) (parse-buffer header-bytes header-size trailing-bytes trailing-size buffer)
      (let ((rest-buffer (concatenate '(vector (unsigned-byte 8)) (decoder-prev-buffer-trail decoder) buffer-head)))
	;;(format t "merged buffer = ~a~%" rest-buffer)
	(setf (decoder-prev-buffer-trail decoder) buffer-trail)
	(unless (= 0 (length rest-buffer))
	  (multiple-value-bind (new-buffer-head merged-data-frames new-buffer-trail) (parse-buffer header-bytes header-size trailing-bytes trailing-size rest-buffer)
	    (declare (ignore new-buffer-head new-buffer-trail))
	    (setq data-frames (append data-frames merged-data-frames)))))                  
      (loop for data-frame in data-frames do
	(ensure-no-msg-makers data-frame header-bytes trailing-bytes 'decode-buffer)
	(push (decode (decrypt data-frame decrypt-fn)) data))
      data)))

(defun checkout-decoder (queue decoder)
  "Decodes and then resets the internal buffer (acc) of decoder"
  (loop for item in (decode-buffer (decoder-acc decoder) decoder) do
    (when (car item) ;; key = NIL is not allowed and reserved for #'send-echo
      (sb-concurrency:enqueue item queue)))
  (setf (decoder-acc decoder) NIL))
  
(defun handle-connection (connection queue server-buffer-size decoder)
  "Accumulate buffer content until critical mass is reached. Then decode it."
  (multiple-value-bind (buffer length peer-addr) (sb-bsd-sockets:socket-receive connection nil server-buffer-size :element-type '(unsigned-byte 8))
    (declare (ignore peer-addr))      
    (setq buffer (subseq buffer 0 length)) ;; remove trailing zeros      
    (setf (decoder-acc decoder) (concatenate '(vector (unsigned-byte 8)) (decoder-acc decoder) buffer))      
    (when (<= (decoder-acc-min-size decoder) (length (decoder-acc decoder)))
      (checkout-decoder queue decoder))))
      
(defun start-udp-server (address port queue settings)
  (setf (network-settings-stop-udp-server settings) NIL)
  (sb-thread:make-thread
   (lambda ()
     (let ((server (make-instance 'sb-bsd-sockets:inet-socket :type :datagram :protocol :udp))
	   (decoder (init-decoder settings)))
       (setf (sb-bsd-sockets:sockopt-reuse-address server) t)
       (handler-case
	   (sb-bsd-sockets:socket-bind server address port)
	 (SB-BSD-SOCKETS:ADDRESS-IN-USE-ERROR (c)
	   (declare (ignore c))
	   NIL))
       (setf (network-settings-udp-socket settings) server)
       (loop until (network-settings-stop-udp-server settings) do	 
	 (handle-connection server queue
			    (network-settings-server-buffer-size settings)
			    decoder))
       (sb-bsd-sockets:socket-close server)))))

(defun start-tcp-server (address port queue settings)
  (setf (network-settings-stop-tcp-server settings) NIL)
  (sb-thread:make-thread
   (lambda ()     
     (let ((server (make-instance 'sb-bsd-sockets:inet-socket :type :stream :protocol :tcp))
	   (decoder (init-decoder settings)))
       (setf (sb-bsd-sockets:sockopt-reuse-address server) t)
       (handler-case
	   (sb-bsd-sockets:socket-bind server address port)
	 (SB-BSD-SOCKETS:ADDRESS-IN-USE-ERROR (c)
	   (declare (ignore c))
	   (progn	     
	     (sb-bsd-sockets:socket-close server)
	     (sleep 1)
	     (start-tcp-server address port queue settings)
	     NIL)))
       (handler-case
	   (sb-bsd-sockets:socket-listen server (network-settings-max-n-of-tcp-connections settings))
	 (SB-BSD-SOCKETS:BAD-FILE-DESCRIPTOR-ERROR (c)
	   (declare (ignore c))
	   (sb-bsd-sockets:socket-close server)
	   (return-from start-tcp-server NIL)))
       (loop until (network-settings-stop-tcp-server settings) do	 
	 (let ((connection (sb-bsd-sockets:socket-accept server)))
	   (sb-thread:make-thread
	    (lambda ()		
	      (loop until (network-settings-stop-tcp-server settings) do
		(handle-connection connection queue
				   (network-settings-server-buffer-size settings)
				   decoder))	      
	      (checkout-decoder queue decoder)
	      (sb-bsd-sockets:socket-close connection)))))
       (sb-bsd-sockets:socket-close server)))))

(defun start-server (addr queue settings)
  (multiple-value-bind (protocol address port) (parse-network-address addr)
    (ccase protocol
     (:tcp (start-tcp-server address port queue settings))
     (:udp (start-udp-server address port queue settings)))))
    
;; remote peer side

;; TCP
(defun try-connect-until-success (socket address port settings)
  (setf (network-settings-stop-tcp-client settings) NIL)
  (sb-thread:make-thread
   (lambda ()
     (let ((number-of-reconnections 0))
       (loop named main until (network-settings-stop-tcp-client settings) do	 
	 (handler-case
	     (progn
	       (sb-bsd-sockets:socket-connect socket address port)
	       (return-from main socket))
	   (SB-BSD-SOCKETS:CONNECTION-REFUSED-ERROR (c)
	     (declare (ignore c))
	     (if (= number-of-reconnections
		    (network-settings-tcp-client-reconnection-attempts settings))
		 (return-from main NIL)
		 (progn
		   (incf number-of-reconnections)
		   (sleep (network-settings-tcp-client-try-reconnect-after settings))))
	     NIL)
	   (SB-BSD-SOCKETS:SOCKET-ERROR (c)
	     (declare (ignore c))
	     (return-from main socket)))))))
  socket)

(defun connect-to-remote-peer (addr settings)
  "Returns node on which one can all send-update"
  (multiple-value-bind (protocol address port) (parse-network-address addr)
    (ccase protocol
      (:tcp
       (let ((client (make-instance 'sb-bsd-sockets:inet-socket :type :stream :protocol :tcp)))
	 (list :tcp address port (try-connect-until-success client address port settings))))
      (:udp (list :udp address port)))))

(defun stop-communication (protocol settings &optional side)
  (ccase protocol
    (:tcp
     (ccase side
       (:server (setf (network-settings-stop-tcp-server settings) t))
       (:client (setf (network-settings-stop-tcp-client settings) t))
       (:all
	(setf (network-settings-stop-tcp-server settings) t)
	(setf (network-settings-stop-tcp-client settings) t))))
     (:udp (setf (network-settings-stop-udp-server settings) t))))

(defun send-update (key value del-p timestamp node settings &optional (retry-p t))
  "Send gvalue data to another node"  
  (let ((data (encode (list key value del-p timestamp)))) ;; send as string
    ;; encrypt if necessary
    (setq data (encrypt data (first (network-settings-encryption-fns settings))))    
    ;; build message frame
    (setq data
	  (concatenate '(vector (unsigned-byte 8))
		       (network-settings-header-bytes settings)
		       data
		       (network-settings-trailing-bytes settings)))    
    ;; check messages before sending    
    (let ((header-bytes (network-settings-header-bytes settings))
	  (header-size (length (network-settings-header-bytes settings)))
	  (trailing-bytes (network-settings-trailing-bytes settings))
	  (trailing-size (length (network-settings-trailing-bytes settings))))				   
      (multiple-value-bind (buffer-head data-frames buffer-trail) (parse-buffer header-bytes header-size trailing-bytes trailing-size data)
	(declare (ignore buffer-head buffer-trail))
	(loop for data-frame in data-frames do
	  (ensure-no-msg-makers data-frame header-bytes trailing-bytes 'send-update))))
    ;; send them
    (ccase (car node)      
      (:tcp
       (handler-case
	   (sb-bsd-sockets:socket-send (car (last node)) data (network-settings-client-buffer-size settings))	     
	 (SB-BSD-SOCKETS:SOCKET-ERROR (c)
	   (declare (ignore c))
	   (sb-bsd-sockets:socket-close (car (last node)))
	   (let ((client (make-instance 'sb-bsd-sockets:inet-socket :type :stream :protocol :tcp)))
	     (try-connect-until-success client (cadr node) (caddr node) settings)
	     ;; update socket in node so that the next call of send-update can succeed
	     (sleep (network-settings-tcp-client-try-reconnect-after settings))
	     (setf (car (last node)) client)
	     ;; resend once
	     (when retry-p	       
	       (send-update key value del-p timestamp node settings NIL))
	     NIL))))
      (:udp
       (handler-case
	   (sb-bsd-sockets:socket-send (network-settings-udp-socket settings) data (network-settings-client-buffer-size settings) :address (list (cadr node) (caddr node)))
	 (SB-BSD-SOCKETS:SOCKET-ERROR (c)
	   (declare (ignore c))
	   ;; resend once
	   (when retry-p
	     (send-update key value del-p timestamp node settings NIL))
	   NIL))))))

(defun send-echo (addr settings)
  "Connects to own tcp server and sends a dummy message.
   Is used to promote server loops by one iteration forward in order to let them close the socket."
  (let ((node (connect-to-remote-peer addr settings)))
    (send-update NIL NIL NIL (init-timestamp 0) node settings)))
