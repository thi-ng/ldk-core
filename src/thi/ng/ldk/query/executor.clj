(ns thi.ng.ldk.query.executor
  (:require
   [thi.ng.ldk.core
    [api :as api]
    [namespaces :as ns]]
   [thi.ng.ldk.common.util :as util]
   [clojure
    [set :as set]
    [pprint :refer [pprint]]]))

(def qvar? #(and (symbol? %) (re-matches #"^\?.*" (name %))))

(def var-name #(-> % name (subs 1)))

(defn resolve-patterns
  [{:keys [prefixes base]} patterns]
  (if (or base prefixes)
    (ns/resolve-patterns prefixes base patterns)
    patterns))

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
             (if include-triple?
               (fn [t]
                 (when (verify t)
                   (-> {:triple t}
                       (inject-res-var t s 0)
                       (inject-res-var t p 1)
                       (inject-res-var t o 2))))
               (fn [t]
                 (when (verify t)
                   (-> {}
                       (inject-res-var t s 0)
                       (inject-res-var t p 1)
                       (inject-res-var t o 2))))))
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
  (comment
    (when (= (count bindings)
             (count (set (vals bindings))))
      bindings))
  bindings)

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

(defn inject-bind-expr
  [res [var expr]]
  (if-let [r (expr res)] (assoc res var r) res))

(defn inject-bindings
  [binds res]
  (map (fn [r] (reduce inject-bind-expr r binds)) res))

(defn- select-join*
  [q [r & more] flt inject]
  (if r
    (lazy-seq (cons r (select-join* q more flt inject)))
    (when-let [pq (peek q)]
      (let [[ds [[p bmap] & patterns] bindings] pq
            res (select-with-bindings ds p bmap false)
            q (pop q)]
        (if (seq res)
          (let [bindings (->> res
                              (map #(unique-var-bindings? (merge bindings %)))
                              (filter (complement nil?)))]
            (if (seq patterns)
              (recur
               (reduce #(queue-queries % patterns %2) q bindings)
               clojure.lang.PersistentVector/EMPTY flt inject)
              (let [bindings (if inject (inject-bindings inject bindings) bindings)
                    bindings (if flt (filter flt bindings) bindings)]
                (recur q bindings flt inject))))
          (recur q clojure.lang.PersistentVector/EMPTY flt inject))))))

(defn select-join
  ([patterns] (select-join patterns {} nil nil))
  ([patterns flt] (select-join patterns {} flt nil))
  ([patterns flt inject] (select-join patterns {} flt inject))
  ([patterns bindings flt inject]
     (select-join*
      (queue-queries clojure.lang.PersistentQueue/EMPTY patterns bindings)
      clojure.lang.PersistentVector/EMPTY flt inject)))

(defn select-join-from
  ([ds patterns] (select-join-from ds patterns {} nil nil))
  ([ds patterns flt] (select-join-from ds patterns {} flt nil))
  ([ds patterns flt inject] (select-join-from ds patterns {} flt inject))
  ([ds patterns bindings flt inject]
     (select-join (map #(cons ds %) (sort-patterns patterns)) bindings flt inject)))
