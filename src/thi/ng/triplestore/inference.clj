(ns thi.ng.triplestore.inference
  (:require
   [thi.ng.triplestore
    [api :as api]
    [query :as q]]
   [clojure
    [set :as set]]))

;; FIXME declare rdf props in api/RDF etc.
(def rules
  [[:sub-property '[[?a "rdfs:subPropertyOf" ?b] [?x ?a ?y]] [['?x '?b '?y]]]
   [:sym-property '[[?a ?r ?b] [?r "rdf:type" "owl:SymmetricProperty"]] [['?b '?r '?a]]]
   [:inv-property-setup '[[?r "owl:inverseOf" ?i]] [['?i "owl:inverseOf" '?r]]]
   [:inv-property '[[?r "owl:inverseOf" ?i] [?a ?r ?b]] [['?b '?i '?a]]]
   ;; FIXME range & domain rules only correct if ?a owl:ObjectProperty
   [:range '[[?a "rdfs:range" ?r] [?x ?a ?y]] '[[?y "rdf:type" ?r] [?r "rdf:type" "owl:Class"]]]
   [:domain '[[?a "rdfs:domain" ?d] [?x ?a ?y]] '[[?x "rdf:type" ?d] [?d "rdf:type" "owl:Class"]]]
   [:sub-class '[[?a "rdfs:subClassOf" ?b] [?x "rdf:type" ?a]] '[[?x "rdf:type" ?b] [?a "rdf:type" "owl:Class"]]]
   [:owl-thing '[[?a "rdf:type" "owl:Class"]] '[[?a "rdfs:subClassOf" "owl:Thing"]]]
   [:trans-property '[[?a "rdf:type" "owl:TransitiveProperty"] [?x ?a ?y] [?y ?a ?z]] '[[?x ?a ?z]]]
   ])

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

(defn infer-all
  ([ds rule targets]
     (infer-all ds rule targets #{}))
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
  infer-all to all rules over `n` passes. Accepts an optional graph
  name `g` as target for inferred triples."
  ([ds rules num-passes]
     (infer-rules ds nil rules num-passes))
  ([ds g rules num-passes]
     (let [rules (if g (map (fn [[id r t]] [id r (map #(cons :inf %) t)]) rules) rules)]
       (loop [ds ds i num-passes]
         (if (zero? i) ds
             (recur
              (reduce
               (fn [ds [id rule targets]]
                 (first (infer-all ds rule targets)))
               ds rules)
              (dec i)))))))

(defn infer-with-annotations
  "Applies infer-all to the given rule and then reifies inferred
  triples using reifiy-as-group with given additional PO couples to
  describe group."
  [ds g rule targets annos]
  (let [targets (map #(cons g %) targets)
        [ds inferred] (infer-all ds rule targets)]
    (api/update-model
     ds g (api/reify-as-group (api/get-model ds g) inferred annos))))
