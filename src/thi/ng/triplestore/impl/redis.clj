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
  (let [h (*hashimpl* e)] (rexec conn (red/hsetnx idx h e)) h))

(defn clear-store
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

(defn get-indexed [conn idx x] (as-node (rexec conn (red/hget idx x))))

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
    (mapcat
     (fn [[ch c]]
       (f conn base-val c (rexec conn (red/hget inner-idx (str base-hash ch)))))
     cmap)))

(defrecord RedisStore [conn]
  api/PModel
  (add-statement [this s p o]
    (add* conn s p o)
    this)
  (subject? [this x]
    (api/as-node (rexec conn (red/hget "subj" (*hashimpl* x)))))
  (predicate? [this x]
    (api/as-node (rexec conn (red/hget "pred" (*hashimpl* x)))))
  (object? [this x]
    (api/as-node (rexec conn (red/hget "obj" (*hashimpl* x)))))
  (select [this s p o]
    (let [[sh ph oh] (map *hashimpl* [s p o])]
      (if s
        (when-let [s (get-indexed conn "subj" sh)]
          (if p
            (when-let [p (get-indexed conn "pred" ph)]
              (when-let [obj (rexec conn (red/hget "spo" (str sh ph)))]
                (if o
                  ;; s p o
                  (when (obj oh) [s p (get-indexed conn "obj" oh)])
                  ;; s p nil
                  (triples-sp conn s p obj))))
            ;; s nil o?
            (let [preds (rexec conn (red/hget "sp" sh))]
              (if o
                (when-let [o (get-indexed conn "obj" oh)]
                  (mapcat
                   (fn [[ph p]]
                     (if (some #(= oh %) (rexec conn (red/hget "spo" (str sh ph)))) [[s (as-node p) o]]))
                   (zipmap preds (rexec conn (apply red/hmget "pred" preds)))))
                (reduce-triples conn triples-sp s sh "spo" "pred" preds)))))
        (if p
          (when-let [p (get-indexed conn "pred" ph)]
            (if o
              ;; nil p o
              (when-let [subj (rexec conn (red/hget "pos" (str ph oh)))]
                (triples-po conn p (get-indexed conn "obj" oh) subj))
              ;; nil p nil
              (reduce-triples conn triples-po p ph "pos" "obj" (rexec conn (red/hget "po" ph)))))
          (if o
            (when-let [preds (rexec conn (red/hget "op" oh))]
              ;; nil nil o
              (reduce-triples conn triples-op (get-indexed conn "obj" oh) oh "ops" "pred" preds))
            ;; nil nil nil
            (mapcat
             (fn [[sh s]]
               (reduce-triples conn triples-sp (get-indexed conn "subj" sh) sh "spo" "pred" (rexec conn (red/hget "sp" sh))))
             (partition 2 (rexec conn (red/hgetall "subj"))))))))))

(defn make-store
  [conn] (RedisStore. conn))
