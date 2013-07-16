(ns thi.ng.triplestore.impl.redis
  (:require
   [thi.ng.triplestore.api :as api]
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

(defrecord RedisStore [conn]
  api/PModel
  (add-statement [this s p o]
    (add* conn s p o)
    this)
  (select [this s p o]
    (let [[sh ph oh] (map *hashimpl* [s p o])]
      (if s
        (when-let [preds (rexec conn (red/hget "sp" sh))]
          (if p
            (let [obj (rexec conn (red/hget "spo" (str sh ph)))]
              (if o
                ;; s p o
                (when (obj oh) [[s p o]])
                ;; s p nil
                (map (fn [o] [s p o]) (rexec conn (apply red/hmget "obj" obj)))))
            ;; s nil o?
            (let [pmap (zipmap preds (rexec conn (apply red/hmget "pred" preds)))]
              (reduce
               (fn [coll [ph p]]
                 (let [obj (rexec conn (red/hget "spo" (str sh ph)))]
                   (if o
                     (if (some #(= oh %) obj) (conj coll [s p o]) coll)
                     (into coll (map (fn [o] [s p o]) (rexec conn (apply red/hmget "obj" obj)))))))
               [] pmap))))
        (if p
          (when-let [obj (rexec conn (red/hget "po" ph))]
            (if o
              ;; nil p o
              (when-let [subj (rexec conn (red/hget "pos" (str ph oh)))]
                (map (fn [s] [s p o]) (rexec conn (apply red/hmget "subj" subj))))
              ;; nil p nil
              (let [omap (zipmap obj (rexec conn (apply red/hmget "obj" obj)))]
                (reduce
                 (fn [coll [oh o]]
                   (let [subj (rexec conn (red/hget "pos" (str ph oh)))]
                     (into coll (map (fn [s] [s p o]) (rexec conn (apply red/hmget "subj" subj))))))
                 [] omap))))
          (if o
            (when-let [preds (rexec conn (red/hget "op" oh))]
              ;; nil nil o
              (let [pmap (zipmap preds (rexec conn (apply red/hmget "pred" preds)))]
                (reduce
                 (fn [coll [ph p]]
                   (let [subj (rexec conn (red/hget "ops" (str oh ph)))]
                     (into coll (map (fn [s] [s p o]) (rexec conn (apply red/hmget "subj" subj))))))
                 [] pmap)))
            ;; nil nil nil
            (mapcat
             (fn [[sh s]]
               (prn sh s)
               (let [preds (rexec conn (red/hget "sp" sh))
                     pmap (zipmap preds (rexec conn (apply red/hmget "pred" preds)))]
                 (prn pmap)
                 (reduce
                  (fn [coll [ph p]]
                    (let [obj (rexec conn (red/hget "spo" (str sh ph)))]
                      (into coll (map (fn [o] [s p o]) (rexec conn (apply red/hmget "obj" obj))))))
                  [] pmap)))
             (partition 2 (rexec conn (red/hgetall "subj"))))))))))

(defn make-store
  [conn] (RedisStore. conn))
