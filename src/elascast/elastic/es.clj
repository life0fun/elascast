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

; the _percolator which holds the repository of registered queries is just a another index.
; the index name that percolator query against for is represented as the type in _percolator index.
; we use q/

; globals
(def ^:dynamic *es-conn*)


(def elasticserver "localhost")
(def elasticport 9200)

(def elascast-index-name "elascast")  ; exports namespace global var

; each document has 3 colns, author, content json, and address fields.
; address has tags list, and include/exclude list. 
(def elascast-mapping-type ["author" "address" "content"])
(def elascast-mapping-type-name "info") ; prescribing highlights of info 

; forward declaration
(declare register-address)
(declare percolate)


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
  "ret mapping scheme with name and type fields"
  [mapping-name mapping-type]
  (let [field-type {:type "string"}  ; each field assoced with field-type
        schema (reduce #(assoc %1 (keyword %2) field-type) {} mapping-type)]
    (hash-map mapping-name {:properties schema})))


; index is db and each mapping types in index is a table.
(defn create-index
  "create index with the passing name and mapping types only"
  [idxname mappings]
  (if-not (esi/exists? idxname)  ; create index only when does not exist
    (esi/create idxname :mappings mappings)))


(defn create-elascast-index
  "create elascast index to store user document with address"
  []
  (let [mapping-type (create-elascast-info-mapping-type 
                        elascast-mapping-type-name
                        elascast-mapping-type)]
    (prn "index mapping-type " mapping-type)
    (create-index elascast-index-name mapping-type)))


; the order of input info json list must match the order of sections.
(defn create-doc
  "create a document by adding address info"
  [author content address]
  (apply merge {:author author} {:address address} {:content content}))


; after submitting the doc, call percolate to find out which query waiting for it.
(defn submit-doc
  "submit an addressable doc into index and percolate the doc"
  [docjson]
  (let [mapping (esi/get-mapping elascast-index-name)
        ;docjson (create-doc author content address)
        ]
    (prn "inserting doc " docjson)
    (esd/create elascast-index-name elascast-mapping-type-name docjson)
    (percolate "percolation" docjson)))  ; percolate the document


; delete doc match all
(defn delete-doc
  "delete doc from the index"
  []
  (esd/delete-by-query elascast-index-name elascast-mapping-type-name (q/match-all)))


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

  
(defn address-query-string 
  "query term in drug index info mapping in field"
  [field keyname]
  (let [now (clj-time/now) 
        pre (clj-time/minus now (clj-time/hours 20))  ; from now back 1 days
        nowfmt (clj-time.format/unparse (clj-time.format/formatters :date-time) now)]
    (q/query-string
      :fields field
      :query (str keyname))))


(defn search
  "search in elascast index using address and key word"
  [address qword]
  (let [search-fields (if (nil? address) "content" address)
        qstring (address-query-string qword)]
    (elastic-query elascast-index-name qstring process-hits)))


; wildcard query: (esd/search "tweets" "tweet" :query {:wildcard {:username "*werkz"}})
(defn query-address
  "form wildcard query to address field (not analyzed)"
  [addr]
  ;(q/term :address term))
  ;(q/field :address (str "*" term "*")))
  (q/wildcard :address (str "*" addr "*")))
  


; called after doc submmited to ES, find out all matched queries for it.
(defn percolate
  "percolate query blocking only when matching doc found"
  [pname docjson]
  (let [response (pcl/percolate elascast-index-name pname :doc docjson)]
    (prn (esrsp/ok? response))
    ;response rets matched query-name, {:ok true, :matches ["client-1" "client-2"]}
    ;(prn response)
    (prn "percolate query matches " (esrsp/matches-from response))))


; unregister address query to percolator
(defn unregister-query-address
  "unregister address query against elascast index to percolator "
  [pname]
  (pcl/unregister-query elascast-index-name pname))


; registered percolator queries are just a doc under _percolator/index-name/query-name.
; one can filter the queries that will be used to percolate a doc, to reduce the number
; of percolate queries needed 
(defn register-query-address
  "register address query against elascast index to percolator "
  [pname address]
  (prn "registering address query " pname address)
  (let [q (query-address address)]
    (pcl/register-query elascast-index-name pname :query q)))
    




