(ns thi.ng.triplestore.inference
  (:require
   [thi.ng.triplestore
    [api: as api]
    [query :as q]]
   [clojure
    [set :as set]]))

(comment
  (map (fn [{:syms [?a ?b ?r]}] [?b ?r ?a])
       (q/select-join-from ds '[[?a ?r ?b] [?r "rdf:type" "owl:SymmetricProperty"]]))

  (q/infer ds '[[?a ?r ?b] [?r "rdf:type" "owl:SymmetricProperty"]] '[[?b ?r ?a]])
  (q/infer ds '[[?a "rdfs:subClassOf" ?b] [?x "rdf:type" ?a]] '[[?x "rdf:type" ?b]])
  (q/infer ds '[[?a "rdfs:subPropertyOf" ?b] [?x ?a ?y]] '[[?x ?b ?y]])
  )

(defn infer
  [ds rule targets]
  (->> rule
       (q/select-join-from ds)
       (mapcat
        (fn [res]
          (map
           (fn [[s p o]]
             [(if (symbol? s) (res s) s)
              (if (symbol? p) (res p) p)
              (if (symbol? o) (res o) o)])
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
         inf))))
