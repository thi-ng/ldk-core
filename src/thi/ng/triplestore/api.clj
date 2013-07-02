(ns thi.ng.triplestore.api)

(defprotocol PModel
  (add-statement [this s p o] [this g s p o])
  (remove-statement [this s p o] [this g s p o])
  (select [this s p o] [this g s p o]))

(defprotocol PDataset
  (add-model [this id m])
  (remove-model [this id]))

(defprotocol PIndexable
  (index-value [this]))

(defprotocol PNode
  (uri [this])
  (label [this])
  (blank? [this])
  (literal? [this])
  (uri? [this]))

(defprotocol PLiteral
  (plain-literal? [this])
  (language [this])
  (datatype [this]))

(defprotocol PResource
  (ns-uri [this])
  (local-uri [this]))

(extend-protocol PIndexable
  String
  (index-value [this] this)
  nil
  (index-value [this] this))
