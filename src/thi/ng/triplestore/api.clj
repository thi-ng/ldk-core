(ns thi.ng.triplestore.api)

(defprotocol PModel
  (add-statement [this s p o] [this g s p o])
  (remove-statement [this s p o] [this g s p o])
  (select [this s p o] [this g s p o]))

(defprotocol PDataset
  (add-model [this id m])
  (remove-model [this id]))

(def ^:dynamic *default-ns-map*
  {"rdf" ""
   "rdfs" ""
   "owl" ""
   "foaf" "http://xmlns.com/foaf/0.1/"
   })

(defn parse-triples
  [store src]
  (reduce
   #(let [[[_ s p o]] (re-seq #"([\w\:]+)\s([\w\:]+)\s(.*)" %2)]
      (add-statement % s p o))
   store
   (clojure.string/split-lines (slurp src))))

