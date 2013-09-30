(ns thi.ng.ldk.io.csv
  (:require
   [thi.ng.ldk.core
    [api :as api]
    [namespaces :as ns :refer [default-namespaces]]]
   [thi.ng.ldk.common.util :as util]
   [clojure
    [string :as str]
    [pprint :refer [pprint]]]
   [clojure.java.io :as io]
   [clojure.data.csv :as csv]))

(defn parse-csv-with
  "Takes a CSV input and fn. Applies the CSV header row vector to fn which
  must return another fn used to convert remaining rows into triples.
  This conversion fn takes 2 args, the row index & column vector,
  and must return a seq of triples or nil to skip a row. The 2-step
  approach is used to allow the converter fn to work with column names
  rather than just indices. See `column-mapper` for further details."
  ([src f] (parse-csv-with src f {}))
  ([src f opts]
     (with-open [in (io/reader src)]
       (let [[hd & more] (csv/read-csv in)
             f (f hd)]
         (doall
          (mapcat
           (fn [[i data]] (f i data))
           (zipmap (range) more)))))))

(defn column-mapper
  "Takes a map of CSV field names to handler fns and optional `row-handler`
  fn and `map-row?` flag, returns a dispatch fn for parse-csv-with, which
  then in turn returns a row converter fn and which dispatches each row field
  to assigned handlers given.

  A field handler fn takes 4 arguments: the row index, field index, field value
  and the complete row, optionally as map with field names as keys
  (controlled by `map-row?`, default true). That way each handler has access
  to the complete row and can return aggregated data, e.g. a computed
  timestamp of a date specified as multiple columns or GPS coordinates of
  separate lat/lon values.

  The optional `row-handler` fn takes 2 args: the original row and the seq of
  transformed values produced by field handlers. The fn can be used a
  post-processing step to further accumulate values and/or as an alternative
  to field handlers."
  ([columns] (column-mapper columns nil true))
  ([columns row-handler] (column-mapper columns row-handler true))
  ([columns row-handler map-row?]
     (fn [header]
       (let [handlers (map-indexed #(or (columns %2) (columns %)) header)]
         (fn [i row]
           (let [rowmap (if map-row? (zipmap header row) row)
                 transformed (mapcat
                              (fn [handler [j data]]
                                (when handler (handler i j data rowmap)))
                              handlers (map-indexed #(vector % %2) row))]
             (if row-handler
               (row-handler row transformed)
               transformed)))))))

(defn apply-template
  [tpl data]
  (let [vars (set (util/filter-tree ex/qvar? tpl))
        [blanks vars] (util/bisect ex/blank-var? vars)
        blanks (zipmap blanks (map (fn [_] (api/make-blank-node)) blanks))
        vars (zipmap vars (map #(get data %) vars))
        subst (merge vars blanks)]
    (map #(replace subst %) tpl)))

(comment
  (def cols {"a" (fn [i j d r] [[j d]]) 2 (fn [_ j d r] [[j d]])})
  (def d ((column-mapper cols) ["b" "a" "c"]))
  (d 1 ["b" "a" "c"])

  (let [tpl '[[?p "a" "foaf:Person"] [?p "foaf:knows/foaf:name" ?n]]
        tpl (ns/resolve-patterns ns/default-namespaces nil tpl)
        mapper (column-mapper
                {} (fn [[p n] _]
                     (apply-template
                      tpl {'?p (api/make-resource p) '?n (api/make-literal n)}))
                false)]
    (parse-csv-with "peeps.csv" mapper)))