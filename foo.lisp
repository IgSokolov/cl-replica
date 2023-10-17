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
	   (setf (socket-option fd :socket :reuseaddr) t)
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
	(buffer (loop for i from 0 to 10 collect i)))
    (unwind-protect
	 (progn
	   (socket-bind fd (make-sockaddr-in))
	   (setf (socket-option fd :socket :reuseaddr) t)
	   (socket-sendto fd
			  (concatenate '(vector (unsigned-byte 8)) buffer)
			  (make-sockaddr-in :addr sendto-addr :port sendto-port))
      (close-socket fd)))))

(defun stop ()
  (setq *stop* t))

(defun echo ()
  (setq *stop* NIL)
  (let ((addr #(127 0 0 1))
	(port 6234)
	(timeout-in-ms 1000))
    (make-thread #'(lambda () (create-udp-server :port port :timeout-in-ms timeout-in-ms :buffer-size 32)))
    (sleep 1)
    (make-thread #'(lambda() (create-udp-client :sendto-addr addr :sendto-port port)))))
    
;;;;;;;;;;;;;;;;;;;


