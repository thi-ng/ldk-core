(ns thi.ng.triplestore.api)

(defprotocol PModel
  (add-statement [this s p o] [this g s p o])
  (remove-statement [this s p o] [this g s p o])
  (select [this s p o] [this g s p o])
  (subjects [this])
  (predicates [this])
  (objects [this])
  (subject? [this x])
  (predicate? [this x])
  (object? [this x])
  (indexed? [this x])
  (prefix-map [this])
  (union [this others])
  (intersection [this others])
  (difference [this others]))

(defprotocol PDataset
  (remove-model [this id])
  (update-model [this id f])
  (get-model [this id]))

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

(defrecord NodeURI [uri]
  PNode
  (blank? [this] false)
  (literal? [this] false)
  (uri? [this] true)
  (uri [this] (.toString uri))
  (label [this] (.toString uri))
  PIndexable
  (index-value [this] (.toString uri)))

(defrecord NodeLiteral [label ^String lang type]
  PNode
  (blank? [this] false)
  (literal? [this] true)
  (uri? [this] false)
  (uri [this] nil)
  (label [this] label)
  PLiteral
  (plain-literal? [this] (nil? type))
  (language [this] lang)
  (datatype [this] type)
  PIndexable
  (index-value [this] (if lang (str label "@" lang) label)))

(defrecord NodeBlank [id]
  PNode
  (blank? [this] true)
  (uri? [this] false)
  (literal? [this] false)
  (uri [this] nil)
  (label [this] id)
  PIndexable
  (index-value [this] id))

(defn make-resource
  [uri] (NodeURI. uri))

(defn make-blank-node
  [] (NodeBlank. (.toString (java.util.UUID/randomUUID))))

(defn make-literal
  ([label] (NodeLiteral. label nil nil))
  ([label lang] (NodeLiteral. label lang nil))
  ([label lang type] (NodeLiteral. label lang type)))

(def RDF
  {:type (make-resource "rdf:type")
   :statement (make-resource "rdf:Statement")
   :subject (make-resource "rdf:subject")
   :predicate (make-resource "rdf:predicate")
   :object (make-resource "rdf:object")
   :membership "rdf:_"
   :alt (make-resource "rdf:Alt")
   :bag (make-resource "rdf:Bag")
   :list (make-resource "rdf:List")
   :seq (make-resource "rdf:Seq")
   :first (make-resource "rdf:first")
   :rest (make-resource "rdf:rest")
   :nil (make-resource "rdf:nil")
   })

(defn add-many
  [store & statements]
  (reduce #(apply add-statement % %2) store statements))

(defn remove-many
  [store & statements]
  (reduce #(apply remove-statement % %2) store statements))

(defn make-container
  ([store c-type items]
     (make-container store c-type (make-blank-node) items))
  ([store c-type node items]
     (->> items
          (map-indexed
           (fn [i v]
             [node (make-resource (str (:membership RDF) (inc i))) v]))
          (cons [node (:type RDF) (c-type RDF)])
          (apply add-many store))))

(defn make-bag
  ([store items] (make-container store :bag items))
  ([store node items] (make-container store :bag node items)))

(defn make-alt
  ([store items] (make-container store :alt (set items)))
  ([store node items] (make-container store :alt node (set items))))

(defn make-seq
  ([store items] (make-container store :seq items))
  ([store node items] (make-container store :seq node items)))

(defn make-list
  ([store items] (make-list store (make-blank-node) items))
  ([store node items]
     (loop [ds store n node [i & more] items]
       (let [stm [[n (:type RDF) (:list RDF)] [n (:first RDF) i]]]
         (if (seq more)
           (let [nxt (make-blank-node)]
             (recur
              (apply add-many ds (concat stm [[n (:rest RDF) nxt]]))
              nxt more))
           (apply add-many ds (concat stm [[n (:rest RDF) (:nil RDF)]])))))))

;; TODO fail if node already exists
(defn reify-statement
  ([store triple] (reify-statement store (make-blank-node) triple))
  ([store node [s p o] & extra]
     (let [store (-> store
                     (add-statement node (:type RDF) (:statement RDF))
                     (add-statement node (:subject RDF) s)
                     (add-statement node (:predicate RDF) p)
                     (add-statement node (:object RDF) o))]
       (apply add-many store (map #(cons node %) extra)))))

(defn reify-as-group
  ([store triples extra] (reify-as-group store (make-blank-node) triples extra))
  ([store node triples extra]
     (let [[store items] (reduce
                          (fn [[ds nodes] t]
                            (let [n (make-blank-node)]
                              [(reify-statement ds n t) (conj nodes n)]))
                          [store []] triples)
           store (make-bag store node items)]
       (apply add-many store (map #(cons node %) extra)))))
