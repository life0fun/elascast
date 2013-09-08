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
                     ""
                ))


(defn create-elascast-index 
  "create elascast index with address and content mapping"
  []
  (prn "creating elascast index with address and content json")
  (es/create-elascast-index))


(defn insert-single-doc
  "insert a doc with address and content json"
  [ docjson ] ; doc json in format {:address {:tag []} :content {:author ... :text ...}}
  (prn "insert-doc " docjson)
  (es/insert-doc docjson))


(defn insert-doc
  "insert docs from txt file. each line is a doc and must be a json"
  [docfile]
  (let [docl (helper/get-doc docfile)]
    (doall insert-single-doc docl)))


(defn -main [& args]
  (doall (map prn help-info))
  (case (first args)
    "create-index" (create-elascast-index)
    "insert-doc" (insert-doc (second args))))