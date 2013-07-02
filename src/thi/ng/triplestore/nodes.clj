(ns thi.ng.triplestore.nodes
  (:require
   [thi.ng.triplestore.api :as api])
  (:import
   [java.util UUID]))

(defrecord NodeURI [uri]
  api/PNode
  (blank? [this] false)
  (literal? [this] false)
  (uri? [this] true)
  (uri [this] (.toString uri))
  (label [this] (.toString uri))
  api/PIndexable
  (index-value [this] (.toString uri)))

(defrecord NodeLiteral [label ^String lang type]
  api/PNode
  (blank? [this] false)
  (literal? [this] true)
  (uri? [this] false)
  (uri [this] nil)
  (label [this] label)
  api/PLiteral
  (plain-literal? [this] (nil? type))
  (language [this] lang)
  (datatype [this] type)
  api/PIndexable
  (index-value [this] (if lang (str label "@" lang) label)))

(defrecord NodeBlank [id]
  api/PNode
  (blank? [this] true)
  (uri? [this] false)
  (literal? [this] false)
  (uri [this] nil)
  (label [this] id)
  api/PIndexable
  (index-value [this] id))

(defn make-resource
  [uri] (NodeURI. uri))

(defn make-blank-node
  [] (NodeBlank. (.toString (UUID/randomUUID))))

(defn make-literal
  ([label] (NodeLiteral. label nil nil))
  ([label lang] (NodeLiteral. label lang nil))
  ([label lang type] (NodeLiteral. label lang type)))
