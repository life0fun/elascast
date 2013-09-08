(defproject elascast "0.1.0-SNAPSHOT"
  :description "An high granularity push engine "
  :url "http://colorcloud.com"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :repositories {"local" ~(str (.toURI (java.io.File. "maven_repository")))}
  :dependencies [
    [org.clojure/clojure "1.5.1"]
    [org.clojure/clojure-contrib "1.2.0"] ; do I stil need the contrib ?
    [clojurewerkz/elastisch "1.1.0"]  ; elastic search API
    [clj-redis "0.0.12"]   ;                           
    [org.clojure/data.json "0.2.2"]    ; json package
    [clj-time "0.5.1"]        ; clj-time wraps Joda time
    [org.clojure/java.jdbc "0.2.3"]         ; jdbc
  ]
  :main elascast.core)
