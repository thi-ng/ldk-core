(ns thi.ng.ldk.io.core
  (:require
   [thi.ng.ldk.core
    [api :as api]]
   [clojure.string :as str]))

(defn parse-triples
  [store src]
  (reduce
   #(let [[[_ s p o]] (re-seq #"([\w\:]+)\s([\w\:]+)\s(.*)" %2)]
      (if (and s p o)
        [(or (api/indexed? % s) (api/make-resource s))
         (or (api/indexed? % p) (api/make-resource p))
         (if (neg? (.indexOf o ":"))
           (or (api/indexed? % o) (api/make-literal o))
           (or (api/indexed? % o) (api/make-resource o)))]
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
                              (string? %) (str "\"" % "\"")
                              (api/blank? %) (format "_:b%04d" (blanks %))
                              (api/uri? %) (let [u (api/label %)]
                                             (if (.startsWith u "http") (str "<" u ">") u))
                              (not (api/plain-literal? %)) (format "\"%s\"@%s" (:label %) (:lang %))
                              :default (str "\"" (:label %) "\""))
                            t))))
         (sort)
         (concat
          (map #(format "@prefix %s: <%s> .\n" (key %) (val %))
               (api/prefix-map store)))
         (apply str)
         (spit f))))

(def dot-default-config
  {:models {:default "blue" :inf "red"}
   :edges {:fontname "Inconsolata"}
   :nodes {:style "filled" :color "black" :fontcolor "white" :fontname "Inconsolata"}})

(defn triples->dot
  [blanks col triples]
  (map (fn [[s p o]]
         (apply format (format "%%s -> %%s[label=%%s, color=\"%s\", fontcolor=\"%s\"];\n"
                               col col)
                (map #(cond
                       (string? %) (str "\"" % "\"")
                       (api/blank? %) (format "\"%04d\"" (blanks %))
                       (api/uri? %) (format "\"%s\"" (api/label %))
                       (:lang %) (format "\"%s@%s\"" (:label %) (:lang %))
                       :default (str "\"" (:label %) "\""))
                     [s o p])))
       triples))

(defn dot-attribs
  [attribs]
  (apply str (interpose "," (map #(format "%s=\"%s\"" (name (key %)) (val %)) attribs))))

(defn write-dot
  [ds {:keys [nodes edges models]} f]
  (let [blanks (zipmap (->> (api/select ds nil nil nil)
                            (mapcat identity)
                            (filter #(when-not (string? %) (api/blank? %)))
                            (set))
                       (range))]
    (->> (if (satisfies? api/PDataset ds)
           (mapcat (fn [k] (triples->dot blanks (models k) (api/select ds k nil nil nil))) (keys (:models ds)))
           (triples->dot blanks (models :default) (api/select ds)))
         (sort)
         (apply str)
         (format "digraph g {\n\nnode[%s];\nedge[%s];\n\n%s}"
                 (dot-attribs nodes)
                 (dot-attribs edges))
         (spit f))))

(defn write-dot-triples
  [q {:keys [nodes edges models]} f]
  (let [blanks (zipmap (->> q
                            (mapcat identity)
                            (filter #(when-not (string? %) (api/blank? %)))
                            (set))
                       (range))]
    (->> (triples->dot blanks (:default models) q)
         (sort)
         (apply str)
         (format "digraph g {\n\nnode[%s];\nedge[%s];\n\n%s}"
                 (dot-attribs nodes)
                 (dot-attribs edges))
         (spit f))))
