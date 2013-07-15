(ns thi.ng.triplestore.impl.memory
  (:require
   [thi.ng.triplestore
    [api :as api]
    [util :as util]
    [namespaces :as ns]]))

(def ^:dynamic *hashimpl* (comp hash api/index-value))

(defn index-entity
  [{idx :idx :as store} e]
  (let [h (*hashimpl* e)]
    (if (idx h)
      [store h]
      [(assoc-in store [:idx h] e) h])))

(defn remove-from-index
  [store idx i1 i2 i3]
  (let [props ((idx store) i1)
        obj (disj (props i2) i3)
        props (if-not (seq obj) (dissoc props i2) props)]
    (if-not (seq props)
      (update-in store [idx] dissoc i1)
      (if-not (seq obj)
        (assoc-in store [idx i1] props)
        (assoc-in store [idx i1 i2] obj)))))

(defn prune-entity-index
  [store e]
  (if-not (or ((:spo store) e) ((:pos store) e) ((:ops store) e))
    (update-in store [:idx] dissoc e)
    store))

(defn triple-sp* [{:keys [idx]} s p] #(vector s p (idx %)))
(defn triple-*po [{:keys [idx]} p o] #(vector (idx %) p o))

(defn select-seq
  ([coll triple-fn & [f-fn]]
     (mapcat (fn [[h objects]]
               (map (triple-fn h) (if f-fn (filter f-fn objects) objects)))
             coll)))

(defrecord MemStore [ns idx spo pos ops]
  api/PModel
  (add-statement [this s p o]
    (let [[this sh] (index-entity this s)
          [this ph] (index-entity this p)
          [this oh] (index-entity this o)]
      (-> this
          (update-in [:spo sh ph] util/eset oh)
          (update-in [:pos ph oh] util/eset sh)
          (update-in [:ops oh ph] util/eset sh))))
  (remove-statement [this s p o]
    (let [[sh ph oh] (map *hashimpl* [s p o])
          props (spo sh)
          obj (get props ph)]
      (if (get obj oh)
        (-> this
            (remove-from-index :spo sh ph oh)
            (remove-from-index :pos ph oh sh)
            (remove-from-index :ops oh ph sh)
            (prune-entity-index sh)
            (prune-entity-index ph)
            (prune-entity-index oh))
        this)))
  (subject? [this x]
    (let [h (*hashimpl* x)] (when (spo h) (idx h))))
  (predicate? [this x]
    (let [h (*hashimpl* x)] (when (pos h) (idx h))))
  (object? [this x]
    (let [h (*hashimpl* x)] (when (ops h) (idx h))))
  (indexed? [this x] (idx (*hashimpl* x)))
  (subjects [this] (map idx (keys spo)))
  (predicates [this] (map idx (keys pos)))
  (objects [this] (map idx (keys ops)))
  (prefix-map [this] ns)
  (select [this s p o]
    (let [[sh ph oh] (map *hashimpl* [s p o])]
      (if s
        (if p
          (if o
            ;; s p o
            (when (get-in spo [sh ph oh]) [[(idx sh) (idx ph) (idx oh)]])
            ;; s p nil
            (when-let [objects (get-in spo [sh ph])]
              (map (triple-sp* this (idx sh) (idx ph)) objects)))
          ;; s nil o / s nil nil
          (when-let [subjects (spo sh)]
            (select-seq subjects #(triple-sp* this (idx sh) (idx %)) (when o #(= oh %)))))
        (if p
          (if o
            ;; nil p o
            (when-let [subjects (get-in pos [ph oh])]
              (map (triple-*po this (idx ph) (idx oh)) subjects))
            ;; nil p nil
            (when-let [preds (pos ph)]
              (select-seq preds #(triple-*po this (idx ph) (idx %)))))
          (if o
            ;; nil nil o
            (when-let [objects (ops oh)]
              (select-seq objects #(triple-*po this (idx %) (idx oh))))
            ;; nil nil nil
            (mapcat
             (fn [[sh props]]
               (select-seq props #(triple-sp* this (idx sh) (idx %))))
             spo))))))
  (union [this others]
    (reduce
     (fn [this m]
       (reduce
        (fn [this [s p o]] (api/add-statement this s p o))
        (update-in this [:ns] merge (api/prefix-map m))
        (api/select m nil nil nil)))
     this (if (satisfies? api/PModel others) [others] others)))
  (intersection [this others]
    (let [others (if (satisfies? api/PModel others) [others] others)]
      (reduce
       (fn [this [s p o]]
         (if (every? #(seq (api/select % s p o)) others)
           this
           (api/remove-statement this s p o)))
       this (api/select this nil nil nil))))
  (difference [this others]
    (let [others (if (satisfies? api/PModel others) [others] others)]
      (reduce
       (fn [this [s p o]]
         (if (some #(seq (api/select % s p o)) others)
           (api/remove-statement this s p o)
           this))
       this (api/select this nil nil nil)))))

(defrecord MemDataset [models]
  api/PModel
  (add-statement [this s p o]
    (api/add-statement this :default s p o))
  (add-statement [this g s p o]
    (update-in this [:models g] api/add-statement s p o))
  (remove-statement [this s p o]
    (api/remove-statement this :default s p o))
  (remove-statement [this g s p o]
    (update-in this [:models g] api/remove-statement s p o))
  (select [this s p o]
    (mapcat #(api/select % s p o) (vals models)))
  (select [this g s p o]
    (api/select (models g) s p o))
  (subject? [this x]
    (some #(api/subject? % x) (vals models)))
  (predicate? [this x]
    (some #(api/predicate? % x) (vals models)))
  (object? [this x]
    (some #(api/object? % x) (vals models)))
  (indexed? [this x]
    (some #(api/indexed? % x) (vals models)))
  (subjects [this]
    (set (mapcat api/subjects (vals models))))
  (predicates [this]
    (set (mapcat api/predicates (vals models))))
  (objects [this]
    (set (mapcat api/objects (vals models))))
  (prefix-map [this]
    (apply merge (map api/prefix-map (vals models))))
  api/PDataset
  (update-model [this id m]
    (assoc-in this [:models id] m))
  (remove-model [this id]
    (update-in this [:models] dissoc id))
  (get-model [this id]
    (models id)))

(defn make-store
  ([] (make-store {}))
  ([prefixes] (MemStore. (merge ns/default-namespaces prefixes) {} {} {} {})))

(defn make-dataset
  ([] (make-dataset {}))
  ([prefixes] (MemDataset. {:default (make-store prefixes)})))

(defn select-from
  [[s p o] triples]
  (api/select (apply api/add-many (make-store) triples) s p o))
