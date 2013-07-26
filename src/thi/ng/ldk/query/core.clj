(ns thi.ng.ldk.query.core
  (:require
   [thi.ng.ldk.core
    [api :as api]
    [namespaces :as ns]]
   [thi.ng.ldk.common.util :as util]
   [thi.ng.ldk.query
    [executor :as q]
    [expressions :as exp]]
   [thi.ng.ldk.store.memory :as mem]
   [clojure
    [set :as set]
    [pprint :refer [pprint]]]))

(defn filter-result-vars
  [vars res]
  (let [vars (if (coll? vars) vars [vars])]
    (map #(select-keys % vars) res)))

(defn format-result-vars
  [results]
  (map
   (fn [r] (into (sorted-map) (map (fn [[k v]] [(-> k q/var-name keyword) v]) r)))
   results))

(defn node-label-or-val
  [x] (if (satisfies? api/PNode x) (api/label x) x))

(defn order-asc
  [vars results]
  (if (coll? vars)
    (sort-by
     (fn [r] (reduce #(conj % (node-label-or-val (r %2))) [] vars))
     results)
    (sort-by
     #(node-label-or-val (get % vars))
     results)))

(defn order-desc
  [vars results]
  (if (coll? vars)
    (sort-by
     (fn [r] (reduce #(conj % (node-label-or-val (r %2))) [] vars))
     #(- (compare ^String % ^String %2))
     results)
    (sort-by
     #(node-label-or-val (get % vars))
     #(- (compare ^String % ^String %2))
     results)))

(defn select-reified
  "Selects all reified statements from store, optional for given triple
  pattern. Returns map of results grouped by reified statment id and
  includes all PO pairs of each."
  ([ds] (select-reified ds nil))
  ([ds triple]
     (let [bindings (->> triple
                         (map #(when-not (nil? %2) [% %2]) '[?subj ?pred ?obj])
                         (into {}))
           res (q/select-join-from
                ds [['?s (:type api/RDF) (:statement api/RDF)]
                    ['?s (:subject api/RDF) '?subj]
                    ['?s (:predicate api/RDF) '?pred]
                    ['?s (:object api/RDF) '?obj]
                    ['?s '?p '?o]]
                bindings nil nil)]
       (->> res
            (group-by #(get % '?s))
            (map
             (fn [[id triples]]
               (let [{s '?subj p '?pred o '?obj} (first triples)]
                 [id {:statement [s p o]
                      :props (format-result-vars (filter-result-vars triples '[?p ?o]))}])))
            (into {})))))

(defn resolve-graph
  [g]
  (if (satisfies? api/PModel g) g (apply api/get-model g)))

(defn remove-generated-vars
  [res]
  (let [gen (filter #(.startsWith (name %) "?___q") (keys res))]
    (apply dissoc res gen)))

(defn process-optional
  [{:keys [optional where graph] :as q} res]
  (let [patterns (q/resolve-patterns q optional)]
    (mapcat
     #(if-let [r (q/select-join-from (resolve-graph graph) patterns % nil nil)] r %)
     res)))

(comment
  (def q
    {:prefixes (api/prefix-map ds2)
     :base "http://thi.ng/owl"
     :select :*
     :graph ds2
     :query [{:where '[[?p "foaf:givenName" ?gn]] :filter '[:not-exists [?p "foaf:surname" "'schmidt'"]] :optional [{:where '[[?p "thi:age" ?a]]}]}]}))

(defn process-select*
  [{{:keys [where optional filter bindings graph]} :query
    :keys [results optional?] :as q}]
  (let [graph (resolve-graph (or graph (:graph q)))
        q* (assoc q :graph graph)
        patterns (q/resolve-patterns q where)
        flt (when filter (first (exp/compile-expression q* [filter])))
        vars (when bindings (exp/compile-expression-map q* bindings))
        ;; _ (prn :patt patterns :flt filter :bind inject)
        ;; _ (prn :prev-res results)
        ;; _ (prn :opt? optional?)
        res (if optional?
              (mapcat
               #(if-let [r (q/select-join-from graph patterns % flt vars)] r [%])
               results)
              (if (seq results)
                (let [res (mapcat #(q/select-join-from graph patterns % flt vars) results)]
                  (when (seq res) res))
                (q/select-join-from graph patterns {} flt vars)))]
    ;; (pprint res)
    ;; (prn "---")
    (reduce
     (fn [q opt] (process-select* (assoc q :query opt :optional? true)))
     (assoc q :results res) optional)))

(defn process-select
  [{:keys [select bindings limit] ord :order ord-a :order-asc ord-d :order-desc :as spec}]
  (let [{res :results} (reduce #(process-select* (assoc % :query %2)) spec (:query spec))
        res (if bindings
              (q/inject-bindings (exp/compile-expression-map spec bindings) res)
              res)
        res (cond
             ord (order-asc ord res)
             ord-a (order-asc ord-a res)
             ord-d (order-desc ord-d res)
             :default res)
        res (if limit (take limit res) res)
        res (if (or (nil? select) (= :* select))
              (map remove-generated-vars res)
              (filter-result-vars select res))]
    res))

(defn process-ask
  [q]
  (when (seq (process-select (assoc q :limit 1))) true))

(defn process-construct
  [{:keys [prefixes construct into] :as q}]
  (let [targets (q/resolve-patterns q construct)
        res (->> (process-select q)
                 (mapcat
                  (fn [res]
                    (map
                     (fn [[s p o]]
                       [(if (symbol? s) (res s) s)
                        (if (symbol? p) (res p) p)
                        (if (symbol? o) (res o) o)])
                     targets)))
                 (set))]
    (if into
      (if (satisfies? api/PModel into)
        (apply api/add-many into res)
        (api/update-model
         (into 0) (into 1)
         (apply api/add-many (apply api/get-model into) res)))
      (apply api/add-many (mem/make-store prefixes) res))))

(comment
  (def q
    {:prefixes {:thi "http://thi.ng/owl#"
                :rel "http://thi.ng/rel#"
                :dc "http://thi.ng/dc#"}
     :base "http://thi.ng/owl"
     :select '[?p ?prj ?lic]
     :order '?prj
     :graph ds
     :where '[[?p "dc:creator" ?prj]
              [?prj "thi:started" ?s]
              [?prj "thi:hasLicense" ?lic]]
     :filter [:not-exists '[?prj "thi:hasLicense" "thi:lgpl"]]
     :bind {'?title [:concat ?prj " (" ?s ")"]}}))

(defn process-query
  [{:keys [prefixes] :as q}]
  (let [type (some #(when (% q) %) [:select :ask :construct :insert :delete])
        q (if prefixes (update-in q [:prefixes] util/stringify-keys) q)]
    (condp = type
      :select (process-select q)
      :ask (process-ask q)
      :construct (process-construct q)
      (prn "unimplemented"))))
