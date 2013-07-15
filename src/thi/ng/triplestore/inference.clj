(ns thi.ng.triplestore.inference
  (:require
   [thi.ng.triplestore
    [api :as api]
    [namespaces :as ns]
    [query :as q]
    [util :as u]]
   [clojure
    [set :as set]]))

;; FIXME declare rdf props in api/RDF etc.
(def rules
  [[:sub-property
    [['?a (:sub-property ns/RDFS) '?b] ['?x '?a '?y]]
    [['?x '?b '?y]]]
   [:sym-property
    '[[?a ?r ?b] [?r (:type ns/RDF) (:sym-property ns/OWL)]]
    [['?b '?r '?a]]]
   [:inv-property-setup
    [['?r (:inverse-of ns/OWL) '?i]]
    [['?i (:inverse-of ns/OWL) '?r]]]
   [:inv-property
    [['?r (:inverse-of ns/OWL) '?i] ['?a '?r '?b]]
    [['?b '?i '?a]]]
   ;; FIXME range & domain rules only correct if ?a owl:ObjectProperty
   [:range
    [['?a (:range ns/RDFS) '?r] ['?x '?a '?y]]
    [['?y (:type ns/RDF) '?r] ['?r (:type ns/RDF) (:class ns/OWL)]]]
   [:domain
    [['?a (:domain ns/RDFS) '?d] ['?x '?a '?y]]
    [['?x (:type ns/RDF) '?d] ['?d (:type ns/RDF) (:class ns/OWL)]]]
   [:sub-class
    [['?a (:sub-class ns/RDFS) '?b] ['?x (:type ns/RDF) '?a]]
    [['?x (:type ns/RDF) '?b] ['?a (:type ns/RDF) (:class ns/OWL)]]]
   [:owl-thing
    [['?a (:type ns/RDF) (:class ns/OWL)]]
    [['?a (:sub-class ns/RDFS) (:thing ns/OWL)]]]
   [:trans-property
    [['?a (:type ns/RDF) (:trans-property ns/OWL)] ['?x '?a '?y] ['?y '?a '?z]]
    [['?x '?a '?z]]]])

(defn spo-pattern
  [ds id]
  (let [{:syms [?s ?p ?o]}
        (first (q/select-join-from
                ds [[id (:subject ns/INF) '?s]
                    [id (:predicate ns/INF) '?p]
                    [id (:object ns/INF) '?o]]))
        var? #(if (.startsWith (api/label %) "?") (symbol (api/label %)) %)]
    [(var? ?s) (var? ?p) (var? ?o)]))

(defn map-rdf-list [ds f id] (map f (api/rdf-list-seq ds id)))

(defn init-rule
  [ds {:syms [?name ?match ?res]}]
  (let [spo-fn (partial spo-pattern ds)
        match (map-rdf-list ds spo-fn ?match)
        res (map-rdf-list ds spo-fn ?res)]
    [(api/label ?name) match res]))

(defn init-rules-from-model
  [ds id]
  (when-let [root ((first (api/select ds id (:rules ns/INF) nil)) 2)]
    (let [r-query [['?rule (:name ns/INF) '?name]
                   ['?rule (:match ns/INF) '?match]
                   ['?rule (:result ns/INF) '?res]]]
      (->> root
           (map-rdf-list ds api/label)
           (map #(first (q/select-join-from ds r-query {'?rule %} nil)))
           (map (partial init-rule ds))))))

(defn infer
  [ds rule targets]
  (->> (q/select-join-from ds rule nil)
       (mapcat
        (fn [res]
          (map
           (fn [t]
             (condp = (count t)
               3 (let [[s p o] t]
                   [(if (symbol? s) (res s) s)
                    (if (symbol? p) (res p) p)
                    (if (symbol? o) (res o) o)])
               4 (let [[g s p o] t]
                   [g (if (symbol? s) (res s) s)
                    (if (symbol? p) (res p) p)
                    (if (symbol? o) (res o) o)])
               nil))
           targets)))
       (set)))

(defn infer-rule
  ([ds rule targets]
     (infer-rule ds rule targets #{}))
  ([ds rule targets inf]
     (let [new-inf (->> inf
                        (set/difference (infer ds rule targets))
                        (filter
                         #(nil? (seq (apply api/select ds (api/remove-context %))))))]
       ;; (clojure.pprint/pprint new-inf)
       (if (seq new-inf)
         (recur
          (apply api/add-many ds new-inf)
          rule targets
          (set/union inf new-inf))
         [ds (map api/remove-context inf)]))))

(defn infer-rules
  "Takes a PModel or PDataset, a number of rule specs and applies
  infer-rule to all rules over `n` passes. Accepts an optional graph
  name `g` as target for inferred triples. Returns 2-elem vector of
  [updated-model inf-map] where inf-map is a map of triples with rule
  IDs as their keys."
  ([ds rules num-passes]
     (infer-rules ds nil rules num-passes))
  ([ds g rules num-passes]
     (let [rules (if g (map (fn [[id r t]] [id r (map #(cons g %) t)]) rules) rules)]
       (loop [state [ds {}] i num-passes]
         (if (zero? i) state
             (recur
              (reduce
               (fn [[ds inf] [id rule targets]]
                 (let [[ds new-inf] (infer-rule ds rule targets)
                       inf (update-in inf [id] #(into (or % #{}) new-inf))]
                   [ds inf]))
               state rules)
              (dec i)))))))

(defn infer-with-annotations
  "Applies infer-rule to the given rule and then reifies inferred
  triples using reifiy-as-group with given additional PO couples to
  describe group. Returns 2-elem vector of [updated-model inferred]"
  ([ds rule targets annos]
     (let [[ds inferred] (infer-rule ds rule targets)]
       [(api/add-reified-group ds inferred annos) inferred]))
  ([ds g rule targets annos]
     (let [[ds inferred] (infer-rule ds rule (map #(cons g %) targets))]
       [(api/update-model
         ds g (api/add-reified-group (api/get-model ds g) inferred annos))
        inferred])))

(defn infer-rules-with-annotations
  ([ds rules anno-fn num-passes]
     (infer-rules-with-annotations ds nil rules anno-fn num-passes))
  ([ds g rules anno-fn num-passes]
     (let [[ds inf-map] (infer-rules ds g rules num-passes)
           ds (reduce
               (fn [ds [id triples]]
                 (if g
                   (api/update-model
                    ds g (api/add-reified-group (api/get-model ds g) triples (anno-fn id triples)))
                   (api/add-reified-group ds triples (anno-fn id triples))))
               ds inf-map)]
       [ds inf-map])))
