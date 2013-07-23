(ns thi.ng.ldk.query.expressions
  (:require
   [thi.ng.ldk.core
    [api :as api]
    [namespaces :as ns]]
   [thi.ng.ldk.query.executor :as q]
   [thi.ng.ldk.common.util :as util])
  (:import
   [javax.xml.datatype XMLGregorianCalendar]))

(defn node-value-type
  [x]
  ;; (prn :nvt x)
  (cond
   (api/literal? x) [(api/literal-value x) (or (api/datatype x) (:string ns/XSD)) :literal x]
   (api/uri? x) [(api/label x) nil :uri x]
   (api/blank? x) [(api/label x) nil :blank x]
   :default nil))

(defn operand-value-type
  [bindings x]
  ;; (prn :ovt x)
  (cond
   (q/qvar? x) (when-let [v (get bindings x)] (node-value-type v))
   (satisfies? api/PNode x) (node-value-type x)
   (fn? x) (recur bindings (x bindings))
   :default  [x (api/xsd-type x) :const]))

(defn interpret-compare
  [op r]
  (cond
   (neg? r) (or (= < op) (= <= op))
   (zero? r) (= = op)
   :default (or (= > op) (= >= op))))

(defn compare-2*
  [op [va ta na] [vb tb nb]]
  (when (and va vb)
    (cond
     (and (ns/numeric-xsd-types ta) (ns/numeric-xsd-types tb)) (op va vb)
     (= (:string ns/XSD) ta tb) (interpret-compare op (.compareTo ^String va ^String vb))
     (= (:date-time ns/XSD) ta tb) (interpret-compare op (.compare ^XMLGregorianCalendar va
                                                                   ^XMLGregorianCalendar vb))
     (or (= :uri na nb) (= :blank na nb)) (interpret-compare op (.compareTo ^String va ^String vb))
     (= (:boolean ns/XSD) ta tb) (interpret-compare op (.compareTo ^Boolean va ^Boolean vb))
     :default nil)))

(defn compare-2
  [op a b]
  (fn [bindings]
    ;; (prn :c2 :ab a b)
    (let [opa (operand-value-type bindings a)
          opb (operand-value-type bindings b)]
      ;; (prn :c2 :va va ta :vb vb tb)
      (compare-2* op opa opb))))

(defn numeric-op-2
  [op a b]
  (fn [bindings]
    (let [[va ta] (operand-value-type bindings a)
          [vb tb] (operand-value-type bindings b)]
      (when (and va vb (ns/numeric-xsd-types ta) (ns/numeric-xsd-types tb))
        (op va vb)))))

(defn and*
  [& conds] #(every? (fn [f] (first (operand-value-type % f))) conds))

(defn or*
  [& conds] #(some (fn [f] (first (operand-value-type % f))) conds))

(defn exists
  [ds patterns] #(seq (q/select-join-from ds patterns % nil nil)))

(defn not-exists
  [ds patterns]
  (let [f (exists ds patterns)] #(not (f %))))

(defn minus
  [ds patterns]
  (let [f (exists ds patterns)]
    (fn [bindings]
      (not ((set (f bindings)) bindings)))))

(defn value-isa?
  [n type] #(= type (nth (operand-value-type % n) 2)))

(defn numeric?
  [n] #(ns/numeric-xsd-types (second (operand-value-type % n))))

(defn lang
  [n]
  #(let [[_ _ t x] (operand-value-type % n)]
     (when (= :literal t) (api/language x))))

(defn iri
  [prefixes base n]
  #(let [[v t nt] (operand-value-type % n)]
     (cond
      (= (:string ns/XSD) t)
      (let [iri (when prefixes (ns/resolve-pname prefixes v))
            iri (if iri iri
                    (when (and base (= \< (first v)))
                      (ns/resolve-iri base (subs v 1 (dec (count v))))))]
        (api/make-resource (or iri v)))
      (= :uri nt) v
      :default nil)))

(defn concat*
  [& args]
  (fn [bindings]
    (let [res (map #(first (operand-value-type bindings %)) args)]
      (apply str res))))

(defn in-set
  [x set]
  (fn [bindings]
    (let [opa (operand-value-type bindings x)]
      (when (first opa)
        (some
         #(compare-2* = opa (operand-value-type bindings %))
         set)))))

(def compare-ops {:< < :<= <= := = :>= >= :> > :!= not=})
(def math-ops {:mul * :div / :add + :sub -})

(defn ensure-args
  [id n args]
  (if (= n (count args))
    args
    (throw
     (IllegalArgumentException.
      (str "wrong number of params for expr " id
           ", got " (count args) ", but expected " n)))))

(declare compile-expression)

(defmulti compile-expr
  (fn [q form]
    ;; (prn :comp-expr form)
    (if (sequential? form)
      (let [op (first form)]
        (cond
         (compare-ops op) :compare
         (math-ops op) :math
         :default op))
      :atom)))

(defmethod compile-expr :atom [_ a] a)

(defmethod compile-expr :compare
  [q [op & args]] (apply compare-2 (compare-ops op) (ensure-args op 2 (compile-expression q args))))

(defmethod compile-expr :math
  [q [op & args]] (apply numeric-op-2 (math-ops op) (ensure-args op 2 (compile-expression q args))))

(defmethod compile-expr :and
  [q [op & args]] (apply and* (compile-expression q args)))

(defmethod compile-expr :or
  [q [op & args]] (apply or* (compile-expression q args)))

(defmethod compile-expr :exists
  [q [op & args]] (exists (:from q) (q/resolve-patterns q args)))

(defmethod compile-expr :not-exists
  [q [op & args]] (not-exists (:from q) (q/resolve-patterns q args)))

(defmethod compile-expr :minus
  [q [op & args]] (minus (:from q) (q/resolve-patterns q args)))

(defmethod compile-expr :blank?
  [q [op & args]] (value-isa? (first (ensure-args :blank? 1 (compile-expression q args))) :blank))

(defmethod compile-expr :uri?
  [q [op & args]] (value-isa? (first (ensure-args :uri? 1 (compile-expression q args))) :uri))

(defmethod compile-expr :literal?
  [q [op & args]] (value-isa? (first (ensure-args :literal? 1 (compile-expression q args))) :literal))

(defmethod compile-expr :numeric?
  [q [op & args]] (numeric? (first (ensure-args :numeric? 1 (compile-expression q args)))))

(defmethod compile-expr :lang
  [q [op & args]] (lang (first (ensure-args :lang 1 (compile-expression q args)))))

(defmethod compile-expr :in
  [q [op & args]]
  (ensure-args :in 2 args)
  (in-set (first (compile-expression q [(first args)])) (compile-expression q (second args))))

(defmethod compile-expr :iri
  [{:keys [prefixes base] :as q} [op & args]]
  (iri prefixes base (first (ensure-args :iri 1 (compile-expression q args)))))

(defmethod compile-expr :uuid
  [q [op & args]]
  (ensure-args :uuid 0 args)
  (fn [_] (api/make-resource (str "urn:uuid:" (util/uuid)))))

(defmethod compile-expr :concat
  [q [op & args]] (apply concat* (compile-expression q args)))

(defmethod compile-expr :default
  [q [op & args]]
  (throw (IllegalArgumentException. (str "error compiling filter, illegal op: " op))))

(defn compile-expression
  [q spec]
  (reduce (fn [stack form] (conj stack (compile-expr q form))) [] spec))

(defn compile-expression-map
  [q specs]
  (->> specs
       (map (fn [[v exp]] [v (first (compile-expression q [exp]))]))
       (into {})))
