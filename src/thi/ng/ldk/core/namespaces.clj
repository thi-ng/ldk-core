(ns thi.ng.ldk.core.namespaces
  (:require
   [thi.ng.ldk.common.util :as util]))

(def default-namespaces
  {"rdf" "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
   "rdfs" "http://www.w3.org/2000/01/rdf-schema#"
   "owl" "http://www.w3.org/2002/07/owl#"
   "foaf" "http://xmlns.com/foaf/0.1/"
   "xsd" "http://www.w3.org/2001/XMLSchema#"
   "inf" "http://owl.thi.ng/inference#"
   })

(defn resolve-iri
  [^String base ^String iri] (if (neg? (.indexOf iri ":")) (str base iri) iri))

(defn resolve-pname
  [prefixes ^String pname]
  (when pname
    (if (string? pname)
      (let [idx (.indexOf pname ":")]
        (when (>= idx 0)
          (when-let [prefix (get prefixes (subs pname 0 idx))]
            (str prefix (subs pname (inc idx))))))
      pname)))

(defn resolve-map
  ([m] (resolve-map default-namespaces m))
  ([prefixes m]
     (reduce-kv (fn [m k v] (assoc m k (resolve-pname prefixes v))) {} m)))

(defn resolve-item
  [prefixes ^String base x]
  (cond
   (symbol? x) x
   (string? x) (condp = (first x)
                 \" (subs x 1 (dec (count x)))
                 \' (subs x 1 (dec (count x)))
                 \< (resolve-iri base (subs x 1 (dec (count x))))
                 (resolve-pname prefixes x))
   :default x))

(defn resolve-patterns
  [prefixes base patterns]
  (map
   (fn [[s p o :as t]]
     (let [ss (resolve-item prefixes base s)
           pp (if (= "a" p)
               (str (default-namespaces "rdf") "type")
               (resolve-item prefixes base p))
           oo (resolve-item prefixes base o)]
       (if (and ss pp oo)
         [ss pp oo]
         (throw (IllegalArgumentException. (str "couldn't resolve pattern: " t))))))
   patterns))

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
    :nil "rdf:nil"}))

(def RDFS
  (resolve-map
   {:sub-property "rdfs:subPropertyOf"
    :sub-class "rdfs:subClassOf"
    :range "rdfs:range"
    :domain "rdfs:domain"}))

(def OWL
  (resolve-map
   {:class "owl:Class"
    :thing "owl:Thing"
    :sym-property "owl:SymmetricProperty"
    :trans-property "owl:TransitiveProperty"
    :inverse-of "owl:inverseOf"}))

(def INF
  (resolve-map
   {:ruleset "inf:RuleSet"
    :rules "inf:rules"
    :name "inf:name"
    :match "inf:match"
    :result "inf:result"
    :subject "inf:subject"
    :predicate "inf:predicate"
    :object "inf:object"}))

(def XSD
  (resolve-map
   {:boolean "xsd:boolean"
    :byte "xsd:byte"
    :short "xsd:short"
    :integer "xsd:integer"
    :int "xsd:int"
    :long "xsd:long"
    :non-positive-integer "xsd:nonPositiveInteger"
    :non-negative-integer "xsd:nonNegativeInteger"
    :positive-integer "xsd:positiveInteger"
    :negative-integer "xsd:negativeInteger"
    :unsigned-byte "xsd:unsignedByte"
    :unsigned-short "xsd:unsignedShort"
    :unsigned-int "xsd:unsignedInt"
    :unsigned-long "xsd:unsignedLong"
    :decimal "xsd:decimal"
    :double "xsd:double"
    :float "xsd:float"
    :string "xsd:string"
    :date-time "xsd:dateTime"}))

(def numeric-xsd-types
  (->> [:integer
        :decimal
        :float
        :double
        :non-positive-integer
        :non-negative-integer
        :positive-integer
        :negative-integer
        :long
        :int
        :short
        :byte
        :unsigned-long
        :unsigned-int
        :unsigned-short
        :unsigned-byte]
       (map XSD)
       (set)))
