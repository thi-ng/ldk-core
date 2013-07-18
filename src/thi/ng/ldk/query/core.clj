(ns thi.ng.ldk.query.core
  (:require
   [thi.ng.ldk.core
    [api :as api]
    [namespaces :as ns]]
   [thi.ng.ldk.common.util :as util]
   [clojure
    [set :as set]
    [pprint :refer [pprint]]]))

(defn inject-bindings
  [p bindings]
  (map #(if (symbol? %) (bindings %) %) p))

(defn extract-var-name
  [tree]
  (apply str (util/filter-tree string? tree)))

(def qvar? #(and (symbol? %) (re-matches #"^\?.*" (name %))))

(defn accumulate-var-values
  "Takes a seq of query result maps and a set of var names. Returns a
  map with all found vars (as keys) and their values (as set)."
  [results vars]
  (reduce
   (fn [vmap t]
     (reduce
      (fn [vmap v]
        (if (nil? (t v))
          vmap
          (update-in vmap [v] util/eset (t v)))) vmap vars))
   {} results))

(defn produce-queries-with-bound-vars
  "Takes a triple pattern (possibly with variables) and a map of
  possible value sets (or single values) for each var. Produces lazy seq
  of resulting triple query patterns using cartesian product of all values.

      (produce-queries-with-bound-vars
        [?a :type ?b]
        {?a #{\"me\" \"you\"} ?b #{\"foo\" \"bar\"})
      => ((\"me\" :type \"foo\") (\"me\" :type \"bar\")
          (\"you\" :type \"foo\") (\"you\" :type \"bar\"))"
  [[s p o] bindings]
  (let [s (or (bindings s) s)
        p (or (bindings p) p)
        o (or (bindings o) o)]
    (if (some set? [s p o])
      (util/cartesian-product
       (if (set? s) s #{s}) (if (set? p) p #{p}) (if (set? o) o #{o}))
      [[s p o]])))

(defn inject-res-var
  "Takes a map `r`, a vector `t`, a var name `v` and an index. If `v`
  is truthy, injects `v` as new key into `r` with its value at (t idx)."
  [r t v idx] (if v (assoc r v (t idx)) r))

(defn triple-verifier
  "Takes a triple pattern (potentially with vars) and a 3-elem boolean
  vector to indicate which SPO is a var. Returns fn which accepts a
  result triple and returns false if any of the vars clash (e.g. a var
  is used multiple times but result has different values in each position
  or likewise, if different vars relate to same values)."
  [[ts tp to] [vars varp varo]]
  (cond
   (and vars varp varo) (cond
                         (= ts tp to) (fn [[rs rp ro]] (= rs rp ro))
                         (= ts tp) (fn [[rs rp ro]] (and (= rs rp) (not= rs ro)))
                         (= ts to) (fn [[rs rp ro]] (and (= rs ro) (not= rs rp)))
                         (= tp to) (fn [[rs rp ro]] (and (= rp ro) (not= rs rp)))
                         :default (fn [_] true))
   (and vars varp) (if (= ts tp)
                     (fn [[rs rp]] (= rs rp))
                     (fn [[rs rp]] (not= rs rp)))
   (and vars varo) (if (= ts to)
                     (fn [[rs _ ro]] (= rs ro))
                     (fn [[rs _ ro]] (not= rs ro)))
   (and varp varo) (if (= tp to)
                     (fn [[_ rp ro]] (= rp ro))
                     (fn [[_ rp ro]] (not= rp ro)))
   :default (fn [_] true)))

(defn select-with-bindings
  ([store t] (select-with-bindings store t {} true))
  ([store [ts tp to :as t] b include-triple?]
     (let [[syms symp symo :as sym] (map symbol? t)
           [qs b] (if syms [nil (assoc b 0 ts)] [ts b])
           [qp b] (if symp [nil (assoc b 1 tp)] [tp b])
           [qo b] (if symo [nil (assoc b 2 to)] [to b])
           {s 0 p 1 o 2} b
           verify (triple-verifier t sym)]
       (->> (api/select store qs qp qo)
            (map
             (fn [t]
               (when (verify t)
                 (-> (if include-triple? {:triple t}  {})
                     (inject-res-var t s 0)
                     (inject-res-var t p 1)
                     (inject-res-var t o 2)))))
            (filter (complement nil?))))))

(defn build-queries-with-prebounds
  [[s p o :as t] bindings]
  (let [bmap (if (bindings s) {0 s} {})
        bmap (if (bindings p) (assoc bmap 1 p) bmap)
        bmap (if (bindings o) (assoc bmap 2 o) bmap)
        queries (produce-queries-with-bound-vars t bindings)]
    (map #(vector % bmap) queries)))

(defn sort-patterns
  "Sorts query patterns in most restrictive order, based on number of vars
  and reference to patterns with single vars."
  [patterns]
  (let [q (map #(let [v (util/filter-tree qvar? %)] [(count v) v %]) patterns)
        singles (set (mapcat second (filter #(= 1 (first %)) q)))]
    (->> q
         (sort-by (fn [[c v]] (- (* c 4) (count (filter singles v)))))
         (map #(nth % 2)))))

;; TODO add support for owl:sameAs aliases?
(defn unique-var-bindings?
  [bindings]
  (when (= (count bindings)
           (count (set (concat (vals bindings)))))
    bindings))

(defn restrict-multi-bindings
  [p bmap bindings]
  (let [p (vec p)]
    (reduce
     (fn [b [k v]] (if (set? (b v)) (assoc b v (p k)) b))
     bindings bmap)))

(defn queue-queries
  [q [[ds & p] & patterns] bindings]
  (let [queries (build-queries-with-prebounds p bindings)]
    (reduce
     (fn [q [p bmap :as patt]]
       (let [r-binds (restrict-multi-bindings p bmap bindings)]
         (conj q [ds (cons patt patterns) r-binds])))
     q queries)))

(defn- select-join*
  [q [r & more] flt]
  (if r
    (lazy-seq (cons r (select-join* q more flt)))
    (when-let [pq (peek q)]
      (let [[ds [[p bmap] & patterns] bindings] pq
            res (select-with-bindings ds p bmap false)]
        (if (seq res)
          (let [bindings (->> res
                              (map #(unique-var-bindings? (merge bindings %)))
                              (filter (complement nil?)))]
            (if (seq patterns)
              (recur
               (reduce #(queue-queries % patterns %2) (pop q) bindings)
               clojure.lang.PersistentVector/EMPTY flt)
              (recur (pop q) (if flt (filter flt bindings) bindings) flt)))
          (recur (pop q) clojure.lang.PersistentVector/EMPTY flt))))))

(defn select-join
  ([patterns] (select-join patterns {} nil))
  ([patterns flt] (select-join patterns {} flt))
  ([patterns bindings flt]
     (select-join*
      (queue-queries clojure.lang.PersistentQueue/EMPTY patterns bindings)
      clojure.lang.PersistentVector/EMPTY flt)))

(defn select-join-from
  ([ds patterns] (select-join-from ds patterns {} nil))
  ([ds patterns flt] (select-join-from ds patterns {} flt))
  ([ds patterns bindings flt]
     (select-join (map #(cons ds %) (sort-patterns patterns)) bindings flt)))

(defn filter-result-vars
  [res vars]
  (let [vars (if (coll? vars) vars [vars])]
    (map #(select-keys % vars) res)))

(defn format-result-vars
  [results]
  (map
   (fn [r] (into (sorted-map) (map (fn [[k v]] [(-> k name (subs 1) keyword) v]) r)))
   results))

(defn select-reified
  "Selects all reified statements from store, optional for given triple
  pattern. Returns map of results grouped by reified statment id and
  includes all PO pairs of each."
  ([ds] (select-reified ds nil))
  ([ds triple]
     (let [bindings (->> triple
                         (map #(when-not (nil? %2) [% %2]) '[?subj ?pred ?obj])
                         (into {}))
           res (select-join-from
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

(defn filter-compare-numeric
  [op a b]
  (let [va? (qvar? a) vb? (qvar? b)
        ;; add multimethod based on literal type to obtain typed value
        cast #(Double/parseDouble (api/label (get % %2)))]
    (cond
      (and va? vb?) #(op (cast % a) (cast % b))
      va? #(op (cast % a) b)
      vb? #(op a (cast % b))
      :default #(op a b))))

(defn filter-and
  [& conds] #(every? (fn [f] (f %)) conds))

(defn filter-or
  [& conds] #(some (fn [f] (f %)) conds))

(defn filter-not-exists
  [ds patterns] #(not (seq (select-join-from ds patterns % nil))))

(defn order-asc
  [vars results]
  (if (coll? vars)
    (sort-by (fn [r] (reduce #(conj % (api/label (r %2))) [] vars)) results)
    (sort-by #(api/label (get % vars)) results)))

(defn order-desc
  [vars results]
  (if (coll? vars)
    (sort-by (fn [r] (reduce #(conj % (api/label (r %2))) [] vars)) #(- (compare % %2)) results)
    (sort-by #(api/label (get % vars)) #(- (compare % %2)) results)))
