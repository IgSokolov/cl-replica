(require "asdf")
(push (uiop/os:getcwd) asdf:*central-registry*)
(ql:quickload :usocket)
(ql:quickload :bordeaux-threads)
(ql:quickload :flexi-streams)
(asdf:load-system :cl-replica)
(asdf:load-system :cl-replica/test)

