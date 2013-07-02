(ns thi.ng.triplestore.impl.memory
  (:require
   [thi.ng.triplestore
    [api :as api]
    [util :as util]
    [namespaces :as ns]]))

(defn index-entity
  [{idx :idx :as store} e]
  (let [h (hash e)]
    (if ((:idx store) e)
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

(defn triple-sp* [store s p] #(vector s p ((:idx store) %)))
(defn triple-*po [store p o] #(vector ((:idx store) %) p o))

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
    (let [[sh ph oh] (map hash [s p o])
          props ((:spo this) sh)
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
  (select [{idx :idx :as this} s p o]
    (let [[sh ph oh] (map hash [s p o])]
      (if s
        (if p
          (if o
            ;; s p o
            (when (get-in spo [sh ph oh]) [[s p o]])
            ;; s p nil
            (when-let [objects (get-in spo [sh ph])]
              (map (triple-sp* this s p) objects)))
          (when-let [subjects (spo sh)]
            (select-seq subjects #(triple-sp* this s (idx %)) (when o #(= oh %)))))
        (if p
          (if o
            ;; nil p o
            (when-let [subjects (get-in pos [ph oh])]
              (map (triple-*po this p o) subjects))
            ;; nil p nil
            (when-let [preds (pos ph)]
              (select-seq preds #(triple-*po this p (idx %)))))
          (if o
            ;; nil nil o
            (when-let [objects (ops oh)]
              (select-seq objects #(triple-*po this (idx %) o)))
            ;; nil nil nil
            (mapcat
             (fn [[sh props]]
               (select-seq props #(triple-sp* this (idx sh) (idx %))))
             spo)))))))

(defrecord MemDataset [models]
  api/PModel
  (add-statement
    [this s p o] (api/add-statement this :default s p o))
  (add-statement
    [this g s p o] (update-in this [:models g] api/add-statement s p o))
  (remove-statement
    [this s p o] (api/remove-statement this :default s p o))
  (remove-statement
    [this g s p o] (update-in this [:models g] api/remove-statement s p o))
  (select
    [this s p o] (mapcat #(api/select % s p o) (vals models)))
  (select
    [this g s p o] (api/select (g models) s p o))
  api/PDataset
  (add-model
    [this id m] (assoc-in this [:models id] m))
  (remove-model
    [this id] (update-in this [:models] dissoc id)))

(defn make-mem-store
  [& {:as ns}] (MemStore. (merge ns/*default-ns-map* ns) {} {} {} {}))

(defn make-mem-dataset
  [& ns] (MemDataset. {:default (apply make-mem-store ns)}))

(defn add-many
  [store & statements]
  (reduce #(apply api/add-statement % %2) store statements))

(defn remove-many
  [store & statements]
  (reduce #(apply api/remove-statement % %2) store statements))

(defn select-from
  [[s p o] triples]
  (api/select (apply add-many (make-mem-store) triples) s p o))
