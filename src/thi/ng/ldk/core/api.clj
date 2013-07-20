(ns thi.ng.ldk.core.api
  (:require
   [thi.ng.ldk.core.namespaces :as ns]))

(defprotocol PModel
  (add-statement [this s p o] [this g s p o])
  (remove-statement [this s p o] [this g s p o])
  (select [this] [this s p o] [this g s p o])
  (subjects [this])
  (predicates [this])
  (objects [this])
  (subject? [this x])
  (predicate? [this x])
  (object? [this x])
  (indexed? [this x])
  (add-prefix [this prefix uri] [this prefix-map])
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
  (label [this] (.toString uri))
  PIndexable
  (index-value [this] (.toString uri)))

(defrecord NodeLiteral [label ^String lang type]
  PNode
  (blank? [this] false)
  (literal? [this] true)
  (uri? [this] false)
  (label [this] label)
  PLiteral
  (plain-literal? [this]
    (and (nil? type) (nil? lang)))
  (language [this] lang)
  (datatype [this] type)
  PIndexable
  (index-value [this]
    (if lang
      (str label "@" lang)
      (if type
        (str label "^^<" type ">")
        label))))

(defrecord NodeBlank [id]
  PNode
  (blank? [this] true)
  (uri? [this] false)
  (literal? [this] false)
  (label [this] id)
  PIndexable
  (index-value [this] id))

(defn remove-context
  [t] (if (= 4 (count t)) (rest t) t))

(defn make-resource
  [uri] (NodeURI. uri))

(defn make-blank-node
  ([] (NodeBlank. (.toString (java.util.UUID/randomUUID))))
  ([id] (NodeBlank. id)))

(defn make-literal
  ([label] (NodeLiteral. (str label) nil nil))
  ([label lang] (NodeLiteral. (str label) lang nil))
  ([label lang type] (NodeLiteral. (str label) lang type)))

(defn as-node
  [x]
  (cond
   (satisfies? PNode x) x
   (map? x) (cond
             (:uri x)   (map->NodeURI x)
             (:id x)    (map->NodeBlank x)
             (:label x) (map->NodeLiteral x)
             :default nil)
   (string? x) (make-literal x)
   :default nil))

(def RDF
  (reduce
   (fn [m k] (assoc m k (make-resource (k ns/RDF))))
   {} [:type :statement :subject :predicate :object
       :alt :bag :list :seq :first :rest :nil]))

(defn add-many
  [store & statements]
  (reduce #(apply add-statement % %2) store statements))

(defn remove-many
  [store & statements]
  (reduce #(apply remove-statement % %2) store statements))

(defn rdf-container-triples
  ([c-type coll] (rdf-container-triples c-type (make-blank-node) coll))
  ([c-type node coll]
     (->> coll
          (map-indexed
           (fn [i v]
             [node (make-resource (str (ns/default-namespaces "rdf") "_" (inc i))) v]))
          (cons [node (:type RDF) (c-type RDF)]))))

(defn rdf-list-triples
  ([coll] (rdf-list-triples (make-blank-node) coll))
  ([node coll]
     (loop [triples [[node (:type RDF) (:list RDF)]] n node [i & more] coll]
       (let [stm [[n (:first RDF) (or i (:nil RDF))]]]
         (if (seq more)
           (let [nxt (make-blank-node)]
             (recur
              (concat triples stm [[n (:rest RDF) nxt]])
              nxt more))
           (concat triples stm [[n (:rest RDF) (:nil RDF)]]))))))

(defn add-container
  ([store c-type coll]
     (add-container store c-type (make-blank-node) coll))
  ([store c-type node coll]
     (apply add-many store (rdf-container-triples c-type node coll))))

(defn add-bag
  ([store coll] (add-container store :bag coll))
  ([store node coll] (add-container store :bag node coll)))

(defn add-alt
  ([store coll] (add-container store :alt (set coll)))
  ([store node coll] (add-container store :alt node (set coll))))

(defn add-seq
  ([store coll] (add-container store :seq coll))
  ([store node coll] (add-container store :seq node coll)))

(defn add-list
  ([store coll] (add-list store (make-blank-node) coll))
  ([store node coll]
     (apply add-many store (rdf-list-triples node coll))))

;; TODO fail if node already exists
(defn add-reified-statement
  ([store triple] (add-reified-statement store (make-blank-node) triple))
  ([store node [s p o] & extra]
     (let [store (-> store
                     (add-statement node (:type RDF) (:statement RDF))
                     (add-statement node (:subject RDF) s)
                     (add-statement node (:predicate RDF) p)
                     (add-statement node (:object RDF) o))]
       (apply add-many store (map #(cons node %) extra)))))

(defn add-reified-group
  ([store triples extra] (add-reified-group store (make-blank-node) triples extra))
  ([store node triples extra]
     (let [[store coll] (reduce
                         (fn [[ds nodes] t]
                           (let [n (make-blank-node)]
                             [(add-reified-statement ds n t) (conj nodes n)]))
                         [store []] triples)
           store (add-bag store node coll)]
       (apply add-many store (map #(cons node %) extra)))))

(defn rdf-list-seq
  [store node]
  (lazy-seq
   (let [triples (select store node nil nil)
         f (some (fn [[_ p o]] (when (= p (:first RDF)) o)) triples)
         r (some (fn [[_ p o]] (when (= p (:rest RDF)) o)) triples)]
     (when f (if r (cons f (rdf-list-seq store r)) [f])))))

(def xsd-factory ^:const (javax.xml.datatype.DatatypeFactory/newInstance))

(defmulti literal-value #(datatype %))

(defmethod literal-value (:integer ns/XSD)
  [x] (try (Integer/parseInt (label x)) (catch Exception e)))

(defmethod literal-value (:float ns/XSD)
  [x] (try (Float/parseFloat (label x)) (catch Exception e)))

(defmethod literal-value (:double ns/XSD)
  [x] (try (Float/parseFloat (label x)) (catch Exception e)))

(defmethod literal-value (:boolean ns/XSD)
  [x] (try (Boolean/parseBoolean (label x)) (catch Exception e)))

(defmethod literal-value (:string ns/XSD)
  [x] (label x))

(defmethod literal-value (:date-time ns/XSD)
  [x] (try (.newXMLGregorianCalendar xsd-factory ^String (label x)) (catch Exception e)))

(defn xsd-type
  [x]
  (cond
   (instance? String x) (:string ns/XSD)
   (instance? Long x) (:long ns/XSD)
   (instance? Double x) (:float ns/XSD)
   (instance? Integer x) (:int ns/XSD)
   (instance? Byte x) (:byte ns/XSD)
   (instance? Short x) (:short ns/XSD)
   (instance? Float x) (:float ns/XSD)
   (instance? Boolean x) (:boolean ns/XSD)
   (instance? clojure.lang.Ratio x) (:decimal ns/XSD)
   :default nil))
