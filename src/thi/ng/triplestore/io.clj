(ns thi.ng.triplestore.io
  (:require
   [thi.ng.triplestore
    [api :as api]
    [nodes :as n]]
   [clojure.string :as str]))

(defn parse-triples
  [store src]
  (reduce
   #(let [[[_ s p o]] (re-seq #"([\w\:]+)\s([\w\:]+)\s(.*)" %2)]
      (if (and s p o)
        (api/add-statement
         %
         (n/make-resource s)
         (n/make-resource p)
         (if (neg? (.indexOf o ":")) (n/make-literal o) (n/make-resource o)))
        %))
   store
   (filter not-empty (str/split-lines (slurp src)))))
