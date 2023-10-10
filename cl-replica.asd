(asdf:defsystem "cl-replica"
  :description "Library for hash-table replication via epidemic algorithms"
  :author "Dr.-Ing. Igor Sokolov"
  :licence "BSD"
  :version "1.0.0"
  :depends-on (:bordeaux-threads :flexi-streams :usocket)
  :components ((:file "packages")
	       (:file "vector-clock" :depends-on ("packages"))
	       (:file "network-io" :depends-on ("packages"))
	       (:file "hashtable-ops" :depends-on ("packages" "vector-clock"))	     
	       (:file "api" :depends-on ("packages" "hashtable-ops" "vector-clock" "network-io"))))

(asdf:defsystem "cl-replica/test"
  :description "Unit-tests for cl-replica"
  :author "Dr.-Ing. Igor Sokolov"
  :licence "BSD"
  :version "1.0.0"
  :depends-on ("cl-replica")
  :components ((:file "packages")
	       (:file "unit-tests")))  
               
