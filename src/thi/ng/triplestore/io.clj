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

(defn write-ttl
  [store f]
  (let [triples (api/select store nil nil nil)
        blanks (zipmap (->> triples
                            (mapcat identity)
                            (filter #(when-not (string? %) (api/blank? %)))
                            (set))
                       (range))]
    (->> triples
         (map (fn [t]
                (apply format "%s %s %s .\n"
                       (map #(cond
                              (string? %) %
                              (api/blank? %) (format "_:b%04d" (blanks %))
                              (api/uri? %) (api/uri %)
                              (not (api/plain-literal? %)) (format "\"%s\"@%s" (:label %) (:lang %))
                              :default (str "\"" (:label %) "\""))
                            t))))
         (sort)
         (concat
          (map #(format "@prefix %s: <%s> .\n" (key %) (val %))
               (api/prefix-map store)))
         (apply str)
         (spit f))))

(defn triples->dot
  [blanks col triples]
  (map (fn [[s p o]]
         (apply format (str "%s -> %s[label=%s, color=\"" col "\"];\n")
                (map #(cond
                       (string? %) (str "\"" % "\"")
                       (api/blank? %) (format "\"%04d\"" (blanks %))
                       (api/uri? %) (format "\"%s\"" (api/uri %))
                       (not (api/plain-literal? %)) (format "\"%s\"@%s" (:label %) (:lang %))
                       :default (str "\"" (:label %) "\""))
                     [s o p])))
       triples))

(defn write-dot
  [ds cols f]
  (let [blanks (zipmap (->> (api/select ds nil nil nil)
                            (mapcat identity)
                            (filter #(when-not (string? %) (api/blank? %)))
                            (set))
                       (range))]
    (->> (mapcat (fn [[k v]] (triples->dot blanks (cols k) (api/select ds k nil nil nil))) (:models ds))
         (sort)
         (apply str)
         (format "digraph g {\n%s}")
         (spit f))))
