(ns thi.ng.triplestore.nodes
  (:require
   [thi.ng.triplestore.api :as api]))

(defrecord NodeURI [uri]
  api/PNode
  (blank? [this] false)
  (literal? [this] false)
  (uri? [this] true)
  (uri [this] (.toString uri))
  (label [this] (.toString uri))
  (index-value [this] (.toString uri)))

(defrecord NodeLiteral [val type ^String lang]
  api/PNode
  (blank? [this] false)
  (literal? [this] true)
  (uri? [this] false)
  (uri [this] nil)
  (label [this] val)
  (index-value [this] (str val "@" lang))
  api/PLiteral
  (plain-literal? [this] (nil? type))
  (language [this] lang)
  (datatype [this] type))

(defrecord NodeBlank [id]
  api/PNode
  (blank? [this] true)
  (uri? [this] false)
  (literal? [this] false)
  (uri [this] nil)
  (label [this] (.toString id))
  (index-value [this] (.toString id)))
