(ns elascast.elastic.es
  (:require [clojure.string :as str])
  (:require [clojure.java.jdbc :as sql])
  (:import [java.io FileReader]
           [java.util Map Map$Entry List ArrayList Collection Iterator HashMap])
  (:require [clj-redis.client :as redis])    ; bring in redis namespace
  (:require [clojure.data.json :as json]
            [clojure.java.io :only [reader writer] :refer [reader writer]])
  (:require [clojurewerkz.elastisch.rest          :as esr]
            [clojurewerkz.elastisch.rest.document :as esd]
            [clojurewerkz.elastisch.rest.index    :as esi]
            [clojurewerkz.elastisch.query         :as q]
            [clojurewerkz.elastisch.rest.response :as esrsp]
            [clojurewerkz.elastisch.rest.percolation :as pcl]  ; percolation
            [clojure.pprint :as pp])
  (:require [clj-time.core :as clj-time :exclude [extend]]
            [clj-time.format :refer [parse unparse formatter]]
            [clj-time.coerce :refer [to-long from-long]]))

; ES doc format: every doc in ES has [] _id, _index, _type, _score, _source.]
; the _source column is where all custom mapping columns are defined. for logstash,
; @source, @tags, @type, @message, @timestamp, @timestamp, ...

; each index(db) has a list of mappings types(tables) that maps doc fields and their core types.
; each mapping has a name and :properties column family, where all columns and col :type and attrs.
; mapping-type {"person" {:properties {:username {:type "string" :store "yes"} :age {}}}}
;               "vip"    {:properties {:vipname {:type "multi_field" :store "yes"} :age {}}}
; column :type "multi_field" allows a list of core_types to apply to the same column during mapping.
;"properties" : {
;   "name" : {
;     "type" : "multi_field",
;     "path": "just_name",  // how multi-field is accessed, apart from the default field
;     "fields" : {
;         "name" : {"type" : "string", "index" : "analyzed"},
;         "untouched" : {"type" : "string", "index" : "not_analyzed"}}

; overall query object format
; {
;     size: # number of results to return (defaults to 10)
;     from: # offset into results (defaults to 0)
;     fields: # list projected fields that should be returned - http://elasticsearch.org/guide/reference/api/search/fields.html
;     sort: # define sort order - see http://elasticsearch.org/guide/reference/api/search/sort.html
;     query: {
;         query_string: { fields: [] query: "query term"}
;     },
;     facets: {
;         # Facets provide summary information about a particular field or fields in the data
;     }
;     # special case for situations where you want to apply filter/query to results but *not* to facets
;     filter: {
;         # filter objects
;         # a filter is a simple "filter" (query) on a specific field.
;         # Simple means e.g. checking against a specific value or range of values
;     },
; }
;
; example query object: {
;  "size": 100, 
;  "fields": ["@tags", "@type", "@message"]
;  "query": {
;     "filtered":{  <= a json obj contains a query and a filter, apply filter to the query result
;       "query":{ 
;         "query_string":{
;           "fields": ["@tags", "@type", "column-name.*"], <= for * match, same name fields must have same type.
;           "default_operator": "OR",
;           "default_field": "@message",
;           "query": " keyword AND @type:finder_core_",
;           "use_dis_max": true}}  <= convert query into a DisMax query
;       "filter": {
;         "range": {
;           "@timestamp": {
;             "from": "2013-05-22T16:10:48Z", "to": "2013-05-23T02:10:48Z"}}}}},
;   "from": 0,
;   "sort": {
;     "@timestamp":{
;       "order": "desc"}},

; For elasticsearch time range query. You can use DateTimeFormatterBuilder, or
; always use (formatters :data-time) formatter.
;

; globals
(def ^:dynamic *es-conn*)


(def elasticserver "localhost")
(def elasticport 9200)

(def elascast-index-name "elascast")  ; exports namespace global var
(def elascast-mapping-type-name "info") ; prescribing highlights of info 


; forward declaration
(declare register-query)


; wrap connecting fn
(defn connect [host port]
  (esr/connect! (str "http://" host ":" port)))


; an index may store documents of different “mapping types”. 
; mapping types can be thought of as column schemas of a table in a db(index)
; each field has a mapping type. A mapping type defines how a field is analyzed, indexed so can be searched.
; each index has one mapping type. index.my_type.my_field. Each (mapping)type can have many mapping definitions.
; curl -XGET localhost:9200/dodgersdata/data/_mapping?pretty=true
; http://www.elasticsearch.org/guide/reference/mapping/core-types/
(defn create-elascast-info-mapping-type
  "ret a mapping type for drug index with all types of string"
  [mapping-name]
  (let [section-type {:type "string"}  ; each section assoced with section-type
        schema (reduce #(assoc %1 (keyword %2) section-type) {} ["address" "content"])]
    (hash-map mapping-name {:properties schema})))


; index is db and each mapping types in index is a table.
(defn create-index
  "create index with the passing name and mapping types only "
  [idxname mappings]
  (if-not (esi/exists? idxname)  ; create index only when does not exist
  (esi/create idxname :mappings mappings)))


; the order of input info json list must match the order of sections.
(defn create-doc
  "create a document by adding address info"
  [content address]
  (apply merge {:address address} {:content content}))


(defn create-elascast-index
  "create elascast index to store user document with address"
  []
  (let [mapping-type (create-elascast-info-mapping-type elascast-mapping-type-name)]
    (prn "index mapping-type " mapping-type)
    (create-index elascast-index-name mapping-type)))


(defn insert-doc
  "insert addressable doc into index "
  [content address]
  (let [mapping (esi/get-mapping elascast-index-name )
        docjson (create-doc content address)]
    (prn "inserting doc " docjson)
    (esd/create elascast-index-name elascast-mapping-type-name docjson)))


(defn address-query-string 
  "query term in drug index info mapping in field"
  [field keyname]
  (let [now (clj-time/now) 
        pre (clj-time/minus now (clj-time/hours 20))  ; from now back 1 days
        nowfmt (clj-time.format/unparse (clj-time.format/formatters :date-time) now)]
    (q/query-string
      :fields field
      :query (str keyname))))


(defn elastic-query [idxname query process-fn]
  "search ES all types of an index with query string args"
  ; if idxname is unknown, we can use search-all-indexes-and-types.  
  ;(connect "localhost" 9200)           
  (let [res (esd/search-all-types idxname   ; drug
              :size 10        ; limits to 10 results
              :query query
              :sort {"timestamp" {"order" "desc"}})  ; order by drug name
         n (esrsp/total-hits res)
         hits (esrsp/hits-from res)  ; searched out docs in hits array
         facets (esrsp/facets-from res)]  ; facets
    (println (format "Total hits: %d" n))
    (process-fn hits)))


(defn process-hits
  "searched out docs are in hits ary, iterate the list"
  [hits]
  (map prn hits))   ; for each item, print
  

(defn search
  "search in elascast index using address and key word"
  [address qword]
  (let [search-fields (if (nil? address) "content" address)
        qstring (address-query-string qword)]
    (elastic-query elascast-index-name qstring process-hits)))