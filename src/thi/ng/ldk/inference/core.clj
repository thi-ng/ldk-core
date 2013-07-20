(ns thi.ng.ldk.inference.core
  (:require
   [thi.ng.ldk.core
    [api :as api]
    [namespaces :as ns]]
   [thi.ng.ldk.query.executor :as q]
   [thi.ng.ldk.io.turtle :as ttl]
   [thi.ng.ldk.common.util :as u]
   [thi.ng.ldk.store.memory :as mem]
   [clojure
    [set :as set]
    [pprint :refer [pprint]]]
   [clojure.java.io :as jio]))

(defn spo-pattern
  [ds id]
  (let [{:syms [?s ?p ?o]}
        (first (q/select-join-from
                ds [[id (:subject ns/INF) '?s]
                    [id (:predicate ns/INF) '?p]
                    [id (:object ns/INF) '?o]]))
        var? #(if (.startsWith ^String (api/label %) "?") (symbol (api/label %)) %)]
    [(var? ?s) (var? ?p) (var? ?o)]))

(defn map-rdf-list [ds f id] (map f (api/rdf-list-seq ds id)))

(defn init-rule
  [ds {:syms [?name ?match ?res]}]
  (let [spo-fn (partial spo-pattern ds)
        match (map-rdf-list ds spo-fn ?match)
        res (map-rdf-list ds spo-fn ?res)]
    [(api/label ?name) match res]))

(defn init-rules-from-model
  ([uri]
     (let [ds (apply api/add-many (mem/make-store) (ttl/parse-triples uri))
           base (ffirst (api/select ds nil (:type ns/RDF) (:ruleset ns/INF)))]
       (init-rules-from-model ds base)))
  ([ds id]
     (when-let [root ((first (api/select ds id (:rules ns/INF) nil)) 2)]
       (let [r-query [['?rule (:name ns/INF) '?name]
                      ['?rule (:match ns/INF) '?match]
                      ['?rule (:result ns/INF) '?res]]]
         (->> root
              (map-rdf-list ds api/label)
              (map #(first (q/select-join-from ds r-query {'?rule %} nil)))
              (map (partial init-rule ds)))))))

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
  "Repeatedly infers rule and adds new triples to `ds` until it
  produces no further results, returns 2-element vector of updated
  `ds` & inferred triples."
  ([ds rule targets]
     (infer-rule ds rule targets #{}))
  ([ds rule targets inf]
     (let [new-inf (->> inf
                        (set/difference (infer ds rule targets))
                        (filter #(nil? (seq (apply api/select ds (api/remove-context %))))))]
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
