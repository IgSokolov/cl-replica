(in-package :cl-replica.vector-clock)

(eval-when (:compile-toplevel :load-toplevel :execute)
  (declaim (optimize (debug 3))))

(defstruct timestamp
  (vclock #() :type simple-vector))

(defun init-timestamp (n-proc)
  (make-timestamp :vclock (make-array n-proc)))

(defun promote-timestamp (ts self)
  (incf (aref (timestamp-vclock ts) self)))

(defun compare-and-update (ts-own ts-received)
  (setf (timestamp-vclock ts-own)
	(coerce 
	 (loop
	    for t1 across (timestamp-vclock ts-own)
	    for t2 across (timestamp-vclock ts-received)
	    collect (max t1 t2))
	 'vector)))

(define-condition concurrent-access (error)
  ((ts-1 :initarg :ts-1
         :reader ts-1)
   (ts-2 :initarg :ts-2
         :reader ts-2))
  (:report (lambda (condition stream)
	     (format stream "Data races detected in vector clocks:~%Vector 1 = ~a~%Vector 2 = ~a~%"
		     (ts-1 condition) (ts-2 condition)))))

(defun vec-length (vec)
  "Calculates the length of the vector"
  (sqrt (reduce #'+ (loop for elt across vec collect (* elt elt)))))

(defun timestamp<= (ts-1 ts-2)
  (let ((expected-result t) ;; if all items are equal
	(start-search)
	(size (array-dimension (timestamp-vclock ts-1) 0)))
    (unless (= size (array-dimension (timestamp-vclock ts-2) 0))
      (error "Vector clock arrays must have the same length!"))
    (handler-case 
	(dotimes (idx size)
	  (let ((t1 (aref (timestamp-vclock ts-1) idx))
		(t2 (aref (timestamp-vclock ts-2) idx)))
	    (unless (= t1 t2)
	      (if start-search
		  (unless (eq expected-result (< t1 t2))	   
		    (error 'concurrent-access :ts-1 ts-1 :ts-2 ts-2))
		  (setq expected-result (< t1 t2)
			start-search t)))))
      (concurrent-access (c)
	(setq expected-result
 	      (< (vec-length (timestamp-vclock (ts-1 c))) (vec-length (timestamp-vclock (ts-2 c)))))))
    expected-result))
