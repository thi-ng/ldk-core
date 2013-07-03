(ns thi.ng.triplestore.io
  (:require
   [thi.ng.triplestore
    [api :as api]]
   [clojure.string :as str]))

(defn parse-triples
  [store src]
  (reduce
   #(let [[[_ s p o]] (re-seq #"([\w\:]+)\s([\w\:]+)\s(.*)" %2)]
      (if (and s p o)
        (api/add-statement
         %
         (or (api/indexed? % s) (api/make-resource s))
         (or (api/indexed? % p) (api/make-resource p))
         (if (neg? (.indexOf o ":"))
           (or (api/indexed? % o) (api/make-literal o))
           (or (api/indexed? % o) (api/make-resource o))))
        %))
   store
   (filter not-empty (str/split-lines (slurp src)))))
