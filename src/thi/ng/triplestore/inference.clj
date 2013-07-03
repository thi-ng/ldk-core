(ns thi.ng.triplestore.inference
  (:require
   [thi.ng.triplestore
    [api :as api]
    [query :as q]]
   [clojure
    [set :as set]]))

(comment
  (map (fn [{:syms [?a ?b ?r]}] [?b ?r ?a])
       (q/select-join-from ds '[[?a ?r ?b] [?r "rdf:type" "owl:SymmetricProperty"]]))

  (inf/infer ds '[[?a ?r ?b] [?r "rdf:type" "owl:SymmetricProperty"]] '[[?b ?r ?a]])
  (inf/infer ds '[[?a "rdfs:subClassOf" ?b] [?x "rdf:type" ?a]] '[[?x "rdf:type" ?b]])
  (inf/infer ds '[[?a "rdfs:subPropertyOf" ?b] [?x ?a ?y]] '[[?x ?b ?y]])
  (inf/infer ds '[[?a "rdfs:range" ?r] [?x ?a ?y]] '[[?y "rdf:type" ?r]])
  (inf/infer ds '[[?a "rdfs:domain" ?d] [?x ?a ?y]] '[[?x "rdf:type" ?d]])
  )

(defn infer
  [ds rule targets]
  (->> rule
       (q/select-join-from ds)
       (mapcat
        (fn [res]
          (map
           (fn [t]
             (condp = (count t)
               3 (let [[s p o] t]
                   [(if (symbol? s) (res s) s)
                    (if (symbol? p) (res p) p)
                    (if (symbol? o) (res o) o)])
               4 (let [[g s p o] t]
                   [g (if (symbol? s) (res s) s)
                    (if (symbol? p) (res p) p)
                    (if (symbol? o) (res o) o)])
               nil))
           targets)))
       (set)))

(defn infer-all
  ([ds rule targets]
     (infer-all ds rule targets #{}))
  ([ds rule targets inf]
     (let [new-inf (set/difference (infer ds rule targets) inf)]
       (if (seq new-inf)
         (recur
          (apply api/add-many ds new-inf)
          rule targets
          (set/union inf new-inf))
         [ds inf]))))
