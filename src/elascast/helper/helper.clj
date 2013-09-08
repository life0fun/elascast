(ns elascast.helper.helper
  (:require [clojure.string :as str])
  (:require [clojure.java.jdbc :as sql])
  (:import [java.io FileReader]
           [java.util Map Map$Entry List ArrayList Collection Iterator HashMap])
  (:require [clj-redis.client :as redis]) ; bring in redis namespace
  (:require [clj-time.core :as clj-time :exclude [extend]]
            [clj-time.format]
            [clj-time.local]))


(defn get-doc 
  "read doc json line by line from data file"
  [datfile]
  (prn "getting doc json from dat file " datfile))
  


