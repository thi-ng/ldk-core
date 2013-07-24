(ns thi.ng.ldk.common.util)

(defn uuid [] (.toString (java.util.UUID/randomUUID)))

(defn eset [e e2] (if (set? e) (conj e e2) #{e2}))

(defn cartesian-product
  "All the ways to take one item from each sequence
  (taken from clojure.contrib.combinatorics)"
  [& seqs]
  (let [v-original-seqs (vec seqs)
        step
        (fn step [v-seqs]
          (let [increment
                (fn [v-seqs]
                  (loop [i (dec (count v-seqs)), v-seqs v-seqs]
                    (if (neg? i) nil
                        (if-let [rst (next (v-seqs i))]
                          (assoc v-seqs i rst)
                          (recur (dec i) (assoc v-seqs i (v-original-seqs i)))))))]
            (when v-seqs
              (cons (map first v-seqs)
                    (lazy-seq (step (increment v-seqs)))))))]
    (when (every? first seqs)
      (lazy-seq (step v-original-seqs)))))

(defn successive-nth
  "Returns a lazyseq of `n`-element vectors, each one containing a
  successive elements of the original collection, which are `step`
  items apart.

      (successive-nth 3 2 [1 2 3 4 5 6 7])
      ;=> ([1 2 3] [3 4 5] [5 6 7])"
  [n step coll]
  (lazy-seq
    (let [s (take n coll)]
      (if (= n (count s))
        (cons (vec s) (successive-nth n step (drop step coll)))))))

(defn filter-tree
  "Applies `f` to root coll and every of its (nested) elements. Returns
  a vector of items for which `f` returned a truthy value."
  [f root]
  (let [walk (fn walk [acc node]
               (cond
                (f node) (conj acc node)
                (coll? node) (reduce walk acc node)
                :default acc))]
    (reduce walk [] root)))

(defn unwrap [s] (subs s 1 (dec (count s))))

(defn wrap [a b s] (str a s b))

(def wrap-iri #(str \< % \>))

(defn stringify-keys
  [m]
  (into
   {} (map (fn [[k v :as e]] (if (keyword? k) [(name k) v] e)) m)))

(defn interval-set
  [& ivals]
  (->> ivals
       (mapcat (fn [v] (if (sequential? v) (range (v 0) (inc (v 1))) [v])))
       (into (sorted-set))))

(defn check-intervals
  [& ivals]
  (let [[ivals const] (reduce
                       (fn [[i c] v]
                         (if (sequential? v) [(conj i v) c] [i (conj c v)]))
                       [[] #{}] ivals)]
    (fn [x]
      (if (const x) x
          (some (fn [[a b]] (and (<= a x) (<= x b))) ivals)))))
