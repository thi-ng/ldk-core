(ns thi.ng.triplestore.query
  (:require
   [thi.ng.triplestore
    [api :as api]
    [util :as util]]
   [clojure
    [set :as set]
    [pprint :refer [pprint]]]))

(defn extract-var-name
  [tree]
  (apply str (util/filter-tree string? tree)))

(def qvar? #(and (symbol? %) (re-matches #"^\?.*" (name %))))

(def match? #(and (map? %) (:match %)))

(defn accumulate-var-values
  "Takes a seq of query result maps and a set of var names. Returns a
  map with all found vars (as keys) and their values (as set)."
  [results vars]
  (reduce
   (fn [vmap t]
     (reduce (fn [vmap v] (if (nil? (t v)) vmap (update-in vmap [v] util/eset (t v)))) vmap vars))
   {} results))

(defn produce-queries-with-bound-vars
  "Takes a triple pattern (possibly with variables) and a map of
  possible values for each var. Produces lazy seq of resulting triple
  query patterns using cartesian product of all var values.

      (produce-queries-with-bound-vars
        [?a :type ?b]
        {?a [\"me\" \"you\"] ?b #{\"foo\" \"bar\"})
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
  [[ts tp to] [syms symp symo]]
  (fn [[rs rp ro]]
    (cond
     (and syms symp symo) (cond
                           (= ts tp to) (= rs rp ro)
                           (= ts tp) (and (= rs rp) (not= rs ro))
                           (= ts to) (and (= rs ro) (not= rs rp))
                           (= tp to) (and (= rp ro) (not= rs rp))
                           :default true)
     (and syms symp) ((if (= ts tp) = not=) rs rp)
     (and syms symo) ((if (= ts to) = not=) rs ro)
     (and symp symo) ((if (= tp to) = not=) rp ro)
     :default true)))

(defn select-with-bindings
  ([store t] (select-with-bindings store t {}))
  ([store [ts tp to :as t] b]
     (let [[syms symp symo :as sym] (map symbol? t)
           [qs b] (if syms [nil (assoc b :s ts)] [ts b])
           [qp b] (if symp [nil (assoc b :p tp)] [tp b])
           [qo b] (if symo [nil (assoc b :o to)] [to b])
           {:keys [s p o]} b
           verify (triple-verifier t sym)]
       (->> (api/select store qs qp qo)
            (map
             (fn [t]
               (when (verify t)
                 (-> {:triple t}
                     (inject-res-var t s 0)
                     (inject-res-var t p 1)
                     (inject-res-var t o 2)))))
            (filter (complement nil?))))))

(defn build-queries-with-prebounds
  [[s p o :as t] bindings]
  (let [bmap (if (bindings s) {:s s} {})
        bmap (if (bindings p) (assoc bmap :p p) bmap)
        bmap (if (bindings o) (assoc bmap :o o) bmap)
        queries (produce-queries-with-bound-vars t bindings)]
    ;;(prn :queries queries)
    ;;(prn :bmap bmap)
    ;;(prn :bindings bindings)
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

(defn binding-combos
  [bindings]
  (map #(zipmap (keys bindings) (map hash-set %))
       (apply util/cartesian-product (vals bindings))))

(defn unique-var-bindings?
  [bindings]
  (when (= (count bindings)
           (count (set (concat (vals bindings)))))
    bindings))

(declare select-join)

(defn select-join*
  [ds [[p bmap] & patterns] bindings]
  ;; (prn :p p :bind bindings)
  (let [res (select-with-bindings ds p bmap)]
    (when (seq res)
      (let [p-vars (util/filter-tree qvar? p)
            new-binds (into #{} (map #(select-keys % p-vars) res))]
        ;; (prn :bindings bindings)
        ;; (prn :new-bind new-binds)
        ;; (prn :b-combos b-combos)
        (if (seq patterns)
          (mapcat (fn [b]
                    (when-let [b (unique-var-bindings? (merge bindings b))]
                      (select-join ds patterns b)))
                  new-binds)
          (map #(merge bindings %) (map #(select-keys % p-vars) res)))))))

(defn select-join
  [ds [p & patterns] bindings]
  (let [queries (build-queries-with-prebounds p bindings)]
    ;;(prn :queries queries)
    (mapcat
     (fn [q]
       ;; (prn :q q :bindings bindings)
       (select-join* ds (cons q patterns) bindings))
     queries)))

(defn filter-results
  [res vars]
  (map
   #(into {}
          (map (fn [[k v]]
                 [(-> k name (subs 1) keyword) (if (set? v) (first v) v)])
               (select-keys % vars)))
   res))
