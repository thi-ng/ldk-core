(ns thi.ng.ldk.query.resultio
  (:require
   [thi.ng.ldk.core
    [api :as api]]
   [thi.ng.ldk.common.util :as util]
   [thi.ng.ldk.query.executor :as q]
   [clojure.string :as str]
   [clojure.data.csv :as csv]
   [clojure.data.json :as json]
   [clojure.java.io :as jio]))

(defn result-value
  [x]
  (if (satisfies? api/PNode x) (api/label x) x))

(defn write-csv
  ([results f] (write-csv results nil f))
  ([results vars f]
     (let [[header vars] (if (seq vars)
                           (if (map? vars)
                             [(vals vars) (keys vars)]
                             [(map q/var-name vars) vars])
                           (let [vars (reduce #(into % (util/filter-tree q/qvar? %2)) #{} results)]
                             [(map #(-> % name (subs 1)) vars) vars]))
           rows (->> results
                     (map (fn [r] (reduce #(conj % (result-value (r %2))) [] vars)))
                     (concat [header]))]
       (with-open [out (jio/writer f)] (csv/write-csv out rows)))))

(defn result-value-json
  [x {:keys [datatypes]}]
  (if (satisfies? api/PNode x)
    (let [label (api/label x)
          res {:value label}]
      (if (api/literal? x)
        (let [res (assoc res :type "literal")]
          (if-let [lang (api/language x)]
            (assoc res :xml:lang lang)
            (if-let [dt (and datatypes (api/datatype x))]
              (assoc res :datatype dt)
              (assoc res :value (api/literal-value x)))))
        (if (api/uri? x)
          (assoc res :type "uri")
          (assoc res :type "bnode"))))
    {:type "literal" :value x}))

(defn result-binding-json
  [vmap bindings opts]
  (->> bindings
       (map (fn [[k v]] [(vmap k) (result-value-json v opts)]))
       (into {})))

(defn write-json
  ([results f] (write-json results nil f))
  ([results vars f & {:as opts}]
     (let [vars (if (nil? vars)
                  (reduce #(into % (util/filter-tree q/qvar? %2)) #{} results)
                  vars)
           vmap (zipmap vars (map q/var-name vars))
           res {:head {:vars (vals vmap)}
                :results {:bindings (map #(result-binding-json vmap % opts) results)}}]
       (spit f (json/write-str res :escape-slash false))
       )))
