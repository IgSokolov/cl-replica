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

(defmacro with-locked-queue (queue timeout &body body)
  (let ((g-queue (gensym))
	(g-timeout (gensym)))
    `(let ((,g-queue ,queue)
	   (,g-timeout ,timeout))
       (with-slots (qlock data) ,g-queue
	 (with-lock-held (qlock :timeout ,g-timeout)
	   (progn ,@body))))))
		 
(defun queue-elements (q)
  (with-locked-queue q 0
    (cdr data)))

(defun empty-queue-p (q)
  (with-locked-queue q 0
    (null (cdr data))))

(defun queue-front (q)
  (with-locked-queue q 0
    (cadr data)))

(defun dequeue (q)
  (with-locked-queue q 0
    (let ((elements  (cdr data)))
      (unless (setf (cdr data) (cdr elements))
	(setf (car data) data))
      (car elements))))

(defun enqueue (item q)
  (with-locked-queue q 0
    (setf (car data) (setf (cdar data) (list item)))))
				
