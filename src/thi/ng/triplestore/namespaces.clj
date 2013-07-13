(ns thi.ng.triplestore.namespaces)

(def default-namespaces
  {"rdf" "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
   "rdfs" "http://www.w3.org/2000/01/rdf-schema#"
   "owl" "http://www.w3.org/2002/07/owl#"
   "foaf" "http://xmlns.com/foaf/0.1/"
   "xsd" "http://www.w3.org/2001/XMLSchema#"
   })

(defn resolve-pname
  [prefixes ^String pname]
  (when pname
    (let [idx (.indexOf pname ":")]
      (when (>= idx 0)
        (when-let [prefix (get prefixes (subs pname 0 idx))]
          (str prefix (subs pname (inc idx))))))))

(defn resolve-map
  ([m] (resolve-map default-namespaces m))
  ([prefixes m]
     (reduce-kv (fn [m k v] (assoc m k (resolve-pname prefixes v))) {} m)))

(def RDF
  (resolve-map
   {:type "rdf:type"
    :statement "rdf:Statement"
    :subject "rdf:subject"
    :predicate "rdf:predicate"
    :object "rdf:object"
    :li "rdf:li"
    :alt "rdf:Alt"
    :bag "rdf:Bag"
    :list "rdf:List"
    :seq "rdf:Seq"
    :first "rdf:first"
    :rest "rdf:rest"
    :nil "rdf:nil"
    }))

(def XSD
  (resolve-map
   {:integer "xsd:integer"
    :decimal "xsd:decimal"
    :double "xsd:double"
    :string "xsd:string"}))
