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
   (fn [r] (into (sorted-map) (map (fn [[k v]] [(-> k name (subs 1) keyword) v]) r)))
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
                bindings nil)]
       (->> res
            (group-by #(get % '?s))
            (map
             (fn [[id triples]]
               (let [{s '?subj p '?pred o '?obj} (first triples)]
                 [id {:statement [s p o]
                      :props (format-result-vars (filter-result-vars triples '[?p ?o]))}])))
            (into {})))))

(defn inject-bind-expr
  [res [var expr]]
  (if-let [r (expr res)] (assoc res var r) res))

(defn resolve-from
  [from]
  (if (satisfies? api/PModel from) from (apply api/get-model from)))

(defn process-optional
  [{:keys [optional where from] :as q} res]
  (let [patterns (q/resolve-patterns q optional)]
    (mapcat
     #(if-let [r (q/select-join-from (resolve-from from) patterns % nil)] r %) res)))

(defn process-bindings
  [binds res]
  (map (fn [r] (reduce inject-bind-expr r binds)) res))

(defn process-select
  [{:keys [select optional from limit] ord :order ord-a :order-asc ord-d :order-desc :as q}
   patterns filter bindings]
  (let [res (q/select-join-from (resolve-from from) patterns filter)
        res (if optional (process-optional q res) res)
        res (if bindings (process-bindings bindings res) res)
        res (if (or (nil? select) (= :* select)) res (filter-result-vars select res))
        res (if limit (take limit res) res)
        res (cond
             ord (order-asc ord res)
             ord-a (order-asc ord-a res)
             ord-d (order-desc ord-d res)
             :default res)]
    res))

(defn process-ask
  [q patterns filter bindings]
  (when (seq (process-select (assoc q :limit 1) patterns filter bindings)) true))

(defn process-construct
  [{:keys [prefixes construct where into] :as q} patterns filter bindings]
  (let [targets (q/resolve-patterns q construct)
        res (->> (process-select q patterns filter bindings)
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
     :from ds
     :where '[[?p "dc:creator" ?prj]
              [?prj "thi:started" ?s]
              [?prj "thi:hasLicense" ?lic]]
     :filter [:not-exists '[?prj "thi:hasLicense" "thi:lgpl"]]
     :bind {'?title [:concat ?prj " (" ?s ")"]}}))

(defn process-query
  [{:keys [prefixes where filter bindings] :as q}]
  (let [type (some #(when (% q) %) [:select :ask :construct :insert :delete])
        q (if prefixes (update-in q [:prefixes] util/stringify-keys) q)
        patterns (q/resolve-patterns q where)
        filter (when filter (first (exp/compile-expression q [filter])))
        bindings (when bindings
                   (->> bindings
                        (map (fn [[v exp]] [v (first (exp/compile-expression q [exp]))]))
                        (into {})))]
    (condp = type
      :select (process-select q patterns filter bindings)
      :ask (process-ask q patterns filter bindings)
      :construct (process-construct q patterns filter bindings)
      (prn "unimplemented"))))


;; 0800 0852233
;; LB 28242854
;; 0800 443311
