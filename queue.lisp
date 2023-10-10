(in-package :cl-replica.queue)

;; Thread-safe version of Peter Norwigs FIFO queues
;; See Norwig, Waters, Implementing queues in Lisp,
;; ACM SIGPLAN Lisp Pointers, October 1991

(defun make-queue% ()
  (let ((q (list NIL)))
    (setf (car q) q)))

(defstruct queue
  (qlock (make-lock))
  (data (make-queue%)))

(defmacro with-locked-queue (queue &body body)
  (let ((g-queue (gensym)))
    `(let ((,g-queue ,queue))
       (with-slots (qlock data) ,g-queue
	 (unwind-protect
	      (progn
		(acquire-lock qlock)
		(progn ,@body))
	   (release-lock qlock))))))
		 
(defun queue-elements (q)
  (with-locked-queue q
    (cdr data)))

(defun empty-queue-p (q)
  (with-locked-queue q
    (null (cdr data))))

(defun queue-front (q)
  (with-locked-queue q
    (cadr data)))

(defun dequeue (q)
  (with-locked-queue q
    (let ((elements  (cdr data)))
      (unless (setf (cdr data) (cdr elements))
	(setf (car data) data))
      (car elements))))

(defun enqueue (item q)
  (with-locked-queue q
    (setf (car data) (setf (cdar data) (list item)))))
