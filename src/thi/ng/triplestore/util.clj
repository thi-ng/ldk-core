(ns thi.ng.triplestore.util)

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

(defn wrap-iri #(str \< % \>))

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
