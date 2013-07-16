(ns thi.ng.triplestore.impl.redis
  (:require
   [thi.ng.triplestore.api :as api :refer [as-node]]
   [taoensso.carmine :as red]))

(def redis-conn {:pool {} :spec {}})

(defmacro rexec [conn & body] `(red/wcar ~conn ~@body))
;; (defmacro rexec [& body] `(red/wcar redis-conn ~@body))

(def ^:dynamic *hashimpl* (comp hash api/index-value))

(defn index-entity
  [conn idx e]
  (let [h (*hashimpl* e)]
    (rexec conn (red/hsetnx idx h e))
    h))

(defn clear
  [conn]
  (rexec conn (red/del "subj" "pred" "obj" "spo" "pos" "ops" "sp" "po" "op")))

(defn add*
  [conn s p o]
  (let [sh (index-entity conn "subj" s)
        ph (index-entity conn "pred" p)
        oh (index-entity conn "obj" o)
        ksp (str sh ph)
        kpo (str ph oh)
        kop (str oh ph)
        sp (or (rexec conn (red/hget "sp" sh)) #{})
        po (or (rexec conn (red/hget "po" ph)) #{})
        op (or (rexec conn (red/hget "op" oh)) #{})
        spo (or (rexec conn (red/hget "spo" ksp)) #{})
        pos (or (rexec conn (red/hget "pos" ksp)) #{})
        ops (or (rexec conn (red/hget "ops" ksp)) #{})]
    (rexec conn
           (red/hset "sp" sh (conj sp ph))
           (red/hset "po" ph (conj po oh))
           (red/hset "op" oh (conj op ph))
           (red/hset "spo" ksp (conj spo oh))
           (red/hset "pos" kpo (conj pos sh))
           (red/hset "ops" kop (conj ops sh)))
    [sh ph oh]))

(defn triples-sp
  [conn s p coll]
  (let [s (as-node s) p (as-node p)]
    (map (fn [o] [s p (as-node o)]) (rexec conn (apply red/hmget "obj" coll)))))

(defn triples-po
  [conn p o coll]
  (let [p (as-node p) o (as-node o)]
    (map (fn [s] [(as-node s) p o]) (rexec conn (apply red/hmget "subj" coll)))))

(defn triples-op
  [conn o p coll]
  (let [o (as-node o) p (as-node p)]
    (map (fn [s] [(as-node s) p o]) (rexec conn (apply red/hmget "subj" coll)))))

(defn reduce-triples
  [conn f base-val base-hash inner-idx coll-idx coll]
  (let [cmap (zipmap coll (rexec conn (apply red/hmget coll-idx coll)))]
    (reduce
     (fn [coll [ch c]]
       (let [inner (rexec conn (red/hget inner-idx (str base-hash ch)))]
         (into coll (f conn base-val c inner))))
     [] cmap)))

(defrecord RedisStore [conn]
  api/PModel
  (add-statement [this s p o]
    (add* conn s p o)
    this)
  (select [this s p o]
    (let [[sh ph oh] (map *hashimpl* [s p o])]
      (if s
        (when (pos? (rexec conn (red/hexists "sp" sh)))
          (if p
            (let [obj (rexec conn (red/hget "spo" (str sh ph)))]
              (if o
                ;; s p o
                (when (obj oh)
                  [(map as-node (rexec conn (red/hget "subj" sh) (red/hget "pred" ph) (red/hget "obj" oh)))])
                ;; (when (obj oh) [[(as-node s) (as-node p) (as-node o)]])
                ;; s p nil
                (triples-sp conn s p obj)))
            ;; s nil o?
            (let [preds (rexec conn (red/hget "sp" sh))
                  pmap (zipmap preds (rexec conn (apply red/hmget "pred" preds)))]
              (reduce
               (fn [coll [ph p]]
                 (let [obj (rexec conn (red/hget "spo" (str sh ph)))]
                   (if o
                     (if (some #(= oh %) obj) (conj coll [s p o]) coll)
                     (into coll (triples-sp conn s p obj)))))
               [] pmap))))
        (if p
          (when (pos? (rexec conn (red/hexists "po" ph)))
            (if o
              ;; nil p o
              (when-let [subj (rexec conn (red/hget "pos" (str ph oh)))]
                (triples-po conn p o subj))
              ;; nil p nil
              (reduce-triples conn triples-po p ph "pos" "obj" (rexec conn (red/hget "po" ph)))))
          (if o
            (when-let [preds (rexec conn (red/hget "op" oh))]
              ;; nil nil o
              (reduce-triples conn triples-op o oh "ops" "pred" preds))
            ;; nil nil nil
            (mapcat
             (fn [[sh s]]
               (reduce-triples conn triples-sp s sh "spo" "pred" (rexec conn (red/hget "sp" sh))))
             (partition 2 (rexec conn (red/hgetall "subj"))))))))))

(defn make-store
  [conn] (RedisStore. conn))
