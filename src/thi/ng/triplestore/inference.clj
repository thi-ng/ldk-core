(ns thi.ng.triplestore.inference
  (:require
   [thi.ng.triplestore
    [api :as api]
    [query :as q]]
   [clojure
    [set :as set]]))

(def rules
  [[:sub-property '[[?a "rdfs:subPropertyOf" ?b] [?x ?a ?y]] [['?x '?b '?y]]]
   [:sym-property '[[?a ?r ?b] [?r "rdf:type" "owl:SymmetricProperty"]] [['?b '?r '?a]]]
   [:inv-property '[[?r "owl:inverseOf" ?i] [?a ?r ?b]] [['?b '?i '?a] ['?i "owl:inverseOf" '?r]]]
   [:range '[[?a "rdfs:range" ?r] [?x ?a ?y]] '[[?y "rdf:type" ?r]]]
   [:domain '[[?a "rdfs:domain" ?d] [?x ?a ?y]] '[[?x "rdf:type" ?d] [?d "rdf:type" "owl:Class"]]]
   [:sub-class '[[?a "rdfs:subClassOf" ?b] [?x "rdf:type" ?a]] '[[?x "rdf:type" ?b] [?a "rdf:type" "owl:Class"]]]
   [:owl-thing '[[?a "rdf:type" "owl:Class"]] '[[?a "rdfs:subClassOf" "owl:Thing"]]]])

(defn infer
  [ds rule targets]
  (->> rule
       (q/select-join-from ds)
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
       ;;(clojure.pprint/pprint new-inf)
       (if (seq new-inf)
         (recur
          (apply api/add-many ds new-inf)
          rule targets
          (set/union inf new-inf))
         [ds (map api/remove-context inf)]))))

(defn infer-with-annotations
  [ds g rule targets annos]
  (let [targets (map #(cons g %) targets)
        [ds inferred] (infer-all ds rule targets)]
    (api/update-model
     ds g (api/reify-as-group (api/get-model ds g) inferred annos))))
