(defpackage foo
  (:use :cl :fsocket :bordeaux-threads-2))

(in-package :foo)

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
(defun create-tcp-server (&key port timeout-in-ms buffer-size)
  (let ((fd (open-socket :type :stream))
        (pc (open-poll))
	(buffer (make-array buffer-size :element-type '(unsigned-byte 8))))
    (unwind-protect
	 (progn
	   ;; bind
	   (socket-bind fd (sockaddr-in nil port))
	   ;; listen
	   (socket-listen fd)
	   (poll-register pc (make-instance 'pollfd
					    :fd fd
					    :events (poll-events :pollin :pollout)))
	   ;; accept
	   (multiple-value-bind (cfd raddr) (socket-accept fd)
	     (cond
	       (cfd
                (format t "Accepted connection from ~A~%" raddr))
                ;;(socket-shutdown cfd)
                ;;(close-socket cfd))
	       (t
                (format t "Accepting...~%")
                (let ((pfds (poll pc :timeout timeout-in-ms)))
		  (if pfds
		      (dolist (event (poll-events (pollfd-revents (first pfds))))
                        (case event
			  (:pollin
			   (format t "Accepted~%")
			   (multiple-value-bind (cfd2 raddr2) (socket-accept fd)
			     (format t "RADDR: ~A~%" raddr2)
			     ;;(socket-shutdown cfd2)
			     ;;(close-socket cfd2)
			     ))))
		      (format t "Timeout~%")
		      ;; todo: retry n times ..
		      )))))
	   ;; receive
	   (loop for i from 0 to 10
		 do ;; loop until *stop*
		    (print "polling")
		    (when (poll pc :timeout timeout-in-ms)
		      (socket-recv fd buffer)		 
		      (format t "buffer = ~a~%" buffer))))
      (close-socket fd)
      (close-poll pc))))

;; client
(defun create-tcp-client (&key addr port timeout-in-ms)
  (let ((fd (open-socket :type :stream))
        (pc (open-poll))
	(buffer (loop for i from 0 to 100 collect i)))
    ;; the fd is now in non-blocking mode 
    (unwind-protect
	 (progn
	   ;; bind
	   (socket-bind fd (make-sockaddr-in :addr addr :port port)) ;; do we need it ?
	   (poll-register pc (make-instance 'pollfd
                                     :fd fd
                                     :events (poll-events :pollin :pollout)))
	   ;; connect
           (cond
             ((socket-connect fd (make-sockaddr-in :addr addr :port port))
              (format t "Connected immediately~%")
              ;;(socket-shutdown fd)
	      )
             (t
              (format t "Connecting...~%")
              (let ((pfds (poll pc :timeout timeout-in-ms)))
		(if pfds
                    (dolist (event (poll-events (pollfd-revents (first pfds))))
                      (case event
			(:pollout (let ((sts (socket-option fd :socket :error)))
                                    (if (zerop sts)
					(format t "Connected~%")
					(format t "Error connecting ~A~%" sts))
				    ;; todo reconnect n times ..
                                    ;;(socket-shutdown fd)
				    ))))
                    (format t "Timeout connecting.~%")
		    ;; todo reconnect n times ..
		    ))))
	   ;; send
	   (print "sending..")
	   (socket-send fd (concatenate '(vector (unsigned-byte 8)) buffer))
	   (print "sent..."))
      (close-poll pc)
      (close-socket fd))))
