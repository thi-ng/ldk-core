(ns thi.ng.triplestore.io
  (:require
   [thi.ng.triplestore.api :as api]
   [clojure.string :as str]))

(defn parse-triples
  [store src]
  (reduce
   #(let [[[_ s p o]] (re-seq #"([\w\:]+)\s([\w\:]+)\s(.*)" %2)]
      (api/add-statement % s p o))
   store
   (filter not-empty (str/split-lines (slurp src)))))
