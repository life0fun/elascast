(ns elascast.helper.helper
  (:require [clojure.string :as str]
            [clojure.java.io :as io])
  (:require [clojure.java.jdbc :as sql])
  (:import [java.io FileReader]
           [java.util Map Map$Entry List ArrayList Collection Iterator HashMap])
  (:require [clj-redis.client :as redis]) ; bring in redis namespace
  (:require [clj-time.core :as clj-time :exclude [extend]]
            [clj-time.format]
            [clj-time.local]))


; string text as content field, and filter namespaces as 
(defn make-map
  "convert a string line read from data into json object"
  [author content filters]   ; destruct assignment
  (let [f (str/split filters #",")
        trmf (map str/trim f)]   ; filter separated by comma
    (hash-map :author author :content content :address trmf)))


; each doc is 3 lines,  content, filters, and separation line
; use partition-all to split line-seq into smaller groups.
(defn get-doc 
  "read doc json line by line from data file"
  [datfile]
  (with-open [rdr (io/reader datfile)]  ; with-open can only take closable things.
    (let [docs (partition-all 4 (line-seq rdr))]
      (doall (map #(make-map (first %) (second %) (nth % 2)) docs))))) ; force eval
  


