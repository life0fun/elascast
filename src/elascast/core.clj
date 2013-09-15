(ns elascast.core
  (:require [clojure.string :as str])
  (:require [clojure.java.jdbc :as sql])
  (:import [java.io FileReader]
           [java.util Map Map$Entry List ArrayList Collection Iterator HashMap])
  (:require [clj-redis.client :as redis]) ; bring in redis namespace
  (:require [clj-time.core :as clj-time :exclude [extend]]
            [clj-time.format]
            [clj-time.local])
  (:require [elascast.elastic.es :as es]
            [elascast.helper.helper :as helper])
  (:gen-class :main true))


(def help-info (list " -------------------------"
                     "lein run populate docfile "
                     "lein run create-elascast-index"
                     "lein run register-address <name> <address>"
                     ""
                ))


(defn create-elascast-index 
  "create elascast index with address and content mapping"
  []
  (prn "creating elascast index with address and content json")
  (es/create-elascast-index))


; doc json in format {:address {:tag []} :content {:author ... :text ...}}
(defn submit-single-doc
  "insert a doc with address and content json"
  [ docjson ]
  (es/submit-doc docjson))


; read lines from docfile
(defn submit-doc
  "insert docs from txt file. each line is a doc and must be a json"
  [docfile]
  (let [docl (helper/get-doc docfile)]
    (map submit-single-doc docl)))

; register 
(defn register-query-address
  "registery address query with query address for doc percolate"
  [qname address] 
  (es/register-query-address qname address))

; deleting _percolator index does not rm registered queries
(defn unregister-query-address
  "registery address query with query address for doc percolate"
  [qname] 
  (es/unregister-query-address qname))


;delete document from index
(defn delete-doc
  "delete all doc from index"
  []
  (es/delete-doc))

(defn -main [& args]
  (doall (map prn help-info))
  (case (first args)
    "create-index" (create-elascast-index)
    ; register-query query-name keyword
    "register" (register-query-address (second args) (last args))
    ; unregister-query query-name
    "unregister" (unregister-query-address (second args))
    ; submit-doc data/events.txt
    "submit-doc" (submit-doc (second args))
    "delete-doc" (delete-doc)))


