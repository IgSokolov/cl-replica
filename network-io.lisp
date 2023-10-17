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
  (:report (lambda (condition stream) (format stream "Error: ~a~%Cannot parse address: ~a~%Please, use the following template: [protocol]://[ip]:[port] (ex. udp://192.168.1.1:5550)" (error-msg condition) (address condition)))))

(defun as-keyword (str)
  (values (intern (string-upcase str) "KEYWORD")))

(defun parse-network-address (addr)
  "Parses udp://192.168.1.1:5550 to (values #(192 168 1 1) 5550)"
  (flet ((delimiterp (c) (or (char= c #\/) (char= c #\.) (char= c #\:))))        
    (let ((addr-as-list (split-string-by-delimiter addr #'delimiterp)))
      (restart-case
	  (progn
	    (unless
		(= 6 (list-length addr-as-list))	   
	      (error 'network-address-parse-error :address addr))
	    (let ((ip-str (subseq addr-as-list 1 5))
		  (port-str (car (last addr-as-list))))
	      (handler-case
		  (cons (make-array 4 :initial-contents (mapcar #'parse-integer ip-str))
			(parse-integer port-str))
		(SB-INT:SIMPLE-PARSE-ERROR (c)	    
		  (error 'network-address-parse-error :error-msg c :address addr))	    
		(TYPE-ERROR (c)
		  (error 'network-address-parse-error :error-msg c :address addr)))))
	(choose-another-address (new-address)
	  :report "Please choose another address (ex. udp://127.0.0.1:5550)"
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
      (enqueue item queue)))
  (setf (decoder-acc decoder) NIL))

(defun start-udp-server (port queue settings)
  (make-thread
   (lambda ()
     (setf (network-settings-stop-udp-server settings) NIL)
     (let ((decoder (init-decoder settings))
	   (fd (open-socket :type :datagram))
	   (buffer (make-array (network-settings-server-buffer-size settings) :element-type '(unsigned-byte 8))))
     (unwind-protect
	  (with-poll (pc)
	    (setf (socket-option fd :socket :reuseaddr) t)
	    (socket-bind fd (make-sockaddr-in :addr #(127 0 0 1) :port port))
	    (setf (network-settings-udp-socket settings) fd)
	    (poll-register pc (make-instance 'pollfd
					     :fd fd
					     :events (poll-events :pollin)))
	    (loop until (network-settings-stop-udp-server settings) do
	      (when (poll pc :timeout (network-settings-time-to-wait-if-no-data settings)))		
		;; Accumulate buffer content until critical mass is reached. Then decode it.
		(handler-case
		    (multiple-value-bind (length peer-addr) (socket-recvfrom fd buffer)
		      (declare (ignore peer-addr))      
		      (setq buffer (subseq buffer 0 length)) ;; remove trailing zeros      
		      (setf (decoder-acc decoder) (concatenate '(vector (unsigned-byte 8)) (decoder-acc decoder) buffer))      
		      (when (<= (decoder-acc-min-size decoder) (length (decoder-acc decoder)))
			(checkout-decoder queue decoder)))
		  (FSOCKET::POSIX-ERROR NIL))))
       (close-socket fd))))))

(defun start-server (addr queue settings)
  (destructuring-bind (address . port) (parse-network-address addr)
    (declare (ignore address))
    (start-udp-server port queue settings)))
    
;; remote peer side

(defun stop-communication (settings)
  (setf (network-settings-stop-udp-server settings) t))

(defun send-update (key value del-p timestamp node settings)
  "Send gvalue data to another node"
  (destructuring-bind (address . port) node
    (let ((data (encode (list key value del-p timestamp))) ;; send as string
	  (sock-addr (make-sockaddr-in :addr address :port port)))
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
      (socket-sendto (network-settings-udp-socket settings) data sock-addr))))
