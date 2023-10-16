(ql:quickload "fsocket")
(ql:quickload "bordeaux-threads")

(defpackage foo
  (:use :cl :fsocket :bordeaux-threads-2))

(in-package :foo)

(defparameter *stop* NIL)

;; udp
;; server
(defun create-udp-server(&key port timeout-in-ms buffer-size)
  (let ((fd (open-socket :type :datagram))
	(buffer (make-array buffer-size :element-type '(unsigned-byte 8))))
    (unwind-protect
	 (with-poll (pc)
	   (socket-bind fd (make-sockaddr-in :addr #(127 0 0 1) :port port))	   
	   (poll-register pc (make-instance 'pollfd
					    :fd fd
					    :events (poll-events :pollin)))
	   (loop for i from 0 to 10
		 do ;; loop until *stop*			
		    (when (poll pc :timeout timeout-in-ms)
		      (socket-recvfrom fd buffer)		 
		      (format t "buffer = ~a~%" buffer)))))
    (close-socket fd)))

(defun create-udp-client (&key sendto-addr sendto-port)
  (let ((fd (open-socket :type :datagram))
	(buffer (loop for i from 0 to 100 collect i)))
    (unwind-protect
	 (progn
	   (socket-bind fd (make-sockaddr-in))
	   (socket-sendto fd
			  (concatenate '(vector (unsigned-byte 8)) buffer)
			  (make-sockaddr-in :addr sendto-addr :port sendto-port))
      (close-socket fd)))))

;; tcp
;; server
(defun rec-tcp-accept (fd pc timeout-in-ms curr max)
  (unless *stop*
    (when (< curr max)  
      (format t "server: try accept..~%")
      (let ((pfds (poll pc :timeout timeout-in-ms)))
	(if pfds
	    (dolist (event (poll-events (pollfd-revents (first pfds))))
              (case event
		(:pollin
		 (format t "server: Accepted~%")
		 (multiple-value-bind (cfd2 raddr) (socket-accept fd)
		   (format t "server: RADDR: ~A~%" raddr)
		   cfd2))))
	    (progn
	      (format t "server: Timeout~%")
	      (rec-tcp-accept fd pc timeout-in-ms (+ 1 curr) max)))))))

(defun rec-tcp-receive (cfd pc buffer timeout-in-ms curr max)
  (unless *stop*
    (when (< curr max)  
      (print "server: polling")
      (when (poll pc :timeout timeout-in-ms)
	(socket-recv cfd buffer)
	buffer))))
	
    
(defun create-tcp-server (&key port timeout-in-ms buffer-size)
  (let ((fd (open-socket :type :stream))
        (pc (open-poll))
	(buffer (make-array buffer-size :element-type '(unsigned-byte 8))))
    (unwind-protect
	 (progn
	   ;; bind
	   (print "server: bind")
	   (setf (socket-option fd :socket :reuseaddr) t)
	   (socket-bind fd (make-sockaddr-in :addr #(127 0 0 1) :port port))	   
	   ;; listen
	   (print "server: listen")
	   (socket-listen fd)
	   ;; accept
	   (print "server: accept")
	   (poll-register pc (make-instance 'pollfd
					    :fd fd
					    :events (poll-events :pollin)))
	   (let ((cfd (rec-tcp-accept fd pc timeout-in-ms 0 5)))
	   ;; receive 
	     (unless *stop*
	       (poll-register pc (make-instance 'pollfd
					      :fd cfd
					      :events (poll-events :pollin)))
	       (rec-tcp-receive cfd pc buffer timeout-in-ms 0 5))
	     ;;(socket-shutdown cfd :receive)
	     )))
    (close-socket fd)
    (close-poll pc)))

;; client
(defun rec-tcp-connect (fd addr pc timeout-in-ms curr max)
  (unless *stop*
    (when (< curr max)      
      (format t "client: try connect..~%")
      (let ((pfds (poll pc :timeout timeout-in-ms)))
	(if pfds
            (dolist (event (poll-events (pollfd-revents (first pfds))))
	      (case event
		(:pollout (let ((sts (socket-option fd :socket :error)))
                            (if (zerop sts)
				(progn
				  (format t "client: Connected~%")
				  (return))
				(progn
				  (format t "client: Error connecting ~A~%" sts)
				  (rec-tcp-connect fd addr pc timeout-in-ms
						   (+ 1 curr) max)))))))
            (progn
	      (format t "client: Timeout connecting.~%")
	      (rec-tcp-connect fd addr pc timeout-in-ms (+ 1 curr) max)))))))

(defun rec-tcp-send (fd buffer addr pc timeout-in-ms curr max)
  (unless *stop*
    (when (< curr max)
      (print "client: sending..")
      (handler-case
	  (progn
	    (socket-send fd buffer)
	    (print "client: sent!"))
	(FSOCKET::POSIX-ERROR (c)
	  (format t "~%client: FSOCKET::POSIX-ERROR: ~a~%" c)
	  (format t "~%client: try reconnect~%")
	  (rec-tcp-connect fd addr pc timeout-in-ms 0 5)
	  (format t "client: try-send-in-1 sec..~%")
	  (sleep 1)
	  (rec-tcp-send fd buffer addr pc timeout-in-ms (+ 1 curr) max))))))

(defun create-tcp-client (&key addr port timeout-in-ms)
  (let ((fd (open-socket :type :stream)) 
        (pc (open-poll))
	(sockaddr (make-sockaddr-in :addr addr :port port))
	(buffer (loop for i from 0 to 100 collect i)))
    ;; the fd is now in non-blocking mode 
    (unwind-protect
	 (progn
	   ;; bind
	   ;;(socket-bind fd (sockaddr-in)) ;; do we need it ?
	   (setf (socket-option fd :socket :reuseaddr) t)
	   (poll-register pc (make-instance 'pollfd
                                     :fd fd
                                     :events (poll-events :pollin :pollout)))
	   ;; connect
           (if (socket-connect fd sockaddr)
               (format t "client: Connected immediately~%")
             (progn
               (format t "client: Connecting...~%")
	       (rec-tcp-connect fd sockaddr pc timeout-in-ms 0 5)))
	   ;; send	   
	   (rec-tcp-send fd (concatenate '(vector (unsigned-byte 8)) buffer)
		     sockaddr pc timeout-in-ms 0 5)))
    (close-poll pc)
    (close-socket fd)))
  
(defun stop ()
  (setq *stop* t))

(defun echo ()
  (setq *stop* NIL)
  (let ((addr #(127 0 0 1))
	(port 6002)
	(timeout-in-ms 5000))
    (make-thread #'(lambda () (create-tcp-client :addr addr :port port :timeout-in-ms timeout-in-ms)))
    (sleep 1)
    (make-thread #'(lambda() (create-tcp-server :port port :timeout-in-ms timeout-in-ms :buffer-size 8)))    
    ))
    
		     
