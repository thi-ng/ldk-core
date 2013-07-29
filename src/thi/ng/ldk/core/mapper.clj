(ns thi.ng.ldk.core.mapper
  (:require
   [thi.ng.ldk.core
    [api :as api]
    [namespaces :as ns]]
   [thi.ng.ldk.common.util :as util]
   [com.stuartsierra.dependency :as dep]))

(defn triple-dependency-graph
  [triples]
  (reduce (fn [g [s _ o]] (dep/depend g o s)) (dep/graph) triples))

(defn filter-roots
  [g coll]
  (filter #(not (seq (dep/immediate-dependencies g %))) coll))

(defn pname-iri
  [prefixes]
  #(if-let [pname (ns/iri-as-pname prefixes (api/label %))]
     pname (api/label %)))

(defn pname-iri-kw
  [prefixes]
  #(if-let [pname (ns/iri-as-pname-kw prefixes (api/label %))]
     pname (api/label %)))

(defn pname-iri-or-value
  [prefixes]
  #(if (satisfies? api/PNode %)
     (if (api/uri? %)
       (if-let [pname (ns/iri-as-pname prefixes (api/label %))]
         pname (api/label %))
       (if (api/literal? %)
         (api/literal-value %)
         (api/label %)))
     %))

(defn make-tree
  [index g tree subj objects]
  (->> objects
       (mapcat #(mapcat (fn [ps] (when (= subj (ps 1)) [[(ps 0) %]])) (index %)))
       (reduce
        (fn [tree [p o]]
          (if-let [o* (seq (dep/immediate-dependents g o))]
            (update-in tree [subj p] util/vec-conj2* (make-tree index g {} o o*))
            (update-in tree [subj p] util/vec-conj2* o)))
        tree)))

(defn triples-as-tree
  [triples & {:keys [subjects preds objects]
              :or {subjects identity preds identity objects identity}}]
  (let [s-idx (util/collect-indexed #(% 0) subjects triples)
        p-idx (util/collect-indexed #(% 1) preds triples)
        o-idx (util/collect-indexed #(% 2) objects triples)
        triples (map (fn [[s p o]] [(s-idx s) (p-idx p) (o-idx o)]) triples)
        index (reduce
               (fn [idx [s p o]] (update-in idx [o] util/set-conj [p s]))
               {} triples)
        g (triple-dependency-graph triples)]
    (->> (vals s-idx)
         (filter-roots g)
         (reduce #(make-tree index g % %2 (dep/immediate-dependents g %2)) {}))))

(defn triple-dependency-graph
  [triples]
  (reduce (fn [g [s _ o]] (dep/depend g o s)) (dep/graph) triples))

(defn filter-roots
  [g coll]
  (filter #(not (seq (dep/immediate-dependencies g %))) coll))

(defn pname-iri
  [prefixes]
  #(if-let [pname (ns/iri-as-pname prefixes (api/label %))]
     pname (api/label %)))

(defn pname-iri-kw
  [prefixes]
  #(if-let [pname (ns/iri-as-pname-kw prefixes (api/label %))]
     pname (api/label %)))

(defn pname-iri-or-value
  [prefixes]
  #(if (satisfies? api/PNode %)
     (if (api/uri? %)
       (if-let [pname (ns/iri-as-pname prefixes (api/label %))]
         pname (api/label %))
       (if (api/literal? %)
         (api/literal-value %)
         (api/label %)))
     %))

(defn make-tree
  [index g tree subj objects]
  (->> objects
       (mapcat #(mapcat (fn [ps] (when (= subj (ps 1)) [[(ps 0) %]])) (index %)))
       (reduce
        (fn [tree [p o]]
          (if-let [o* (seq (dep/immediate-dependents g o))]
            (update-in tree [subj p] util/vec-conj2* (make-tree index g {} o o*))
            (update-in tree [subj p] util/vec-conj2* o)))
        tree)))

(defn triples-as-tree
  [triples & {:keys [subjects preds objects]
              :or {subjects identity preds identity objects identity}}]
  (let [s-idx (util/collect-indexed #(% 0) subjects triples)
        p-idx (util/collect-indexed #(% 1) preds triples)
        o-idx (util/collect-indexed #(% 2) objects triples)
        triples (map (fn [[s p o]] [(s-idx s) (p-idx p) (o-idx o)]) triples)
        index (reduce
               (fn [idx [s p o]] (update-in idx [o] util/set-conj [p s]))
               {} triples)
        g (triple-dependency-graph triples)]
    (->> (vals s-idx)
         (filter-roots g)
         (reduce #(make-tree index g % %2 (dep/immediate-dependents g %2)) {}))))

(comment
  (q/triples-as-tree
   [[:a :p1 :b] [:a :p1 :g] [:a :p2 :c] [:b :p3 :d] [:d :p4 :e] [:b :p5 :f] [:g :p2 :h] [:g :p2 :hh]])

  ;; =>
  {:a
   {:p1 [{:b {:p3 {:d {:p4 :e}}
              :p5 :f}}
         {:g {:p2 [:hh :h]}}]
    :p2 :c}})
