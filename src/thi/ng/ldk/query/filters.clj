(ns thi.ng.ldk.query.filters
  (:require
   [thi.ng.ldk.core
    [api :as api]
    [namespaces :as ns]]
   [thi.ng.ldk.query.executor :as q])
  (:import
   [javax.xml.datatype XMLGregorianCalendar]))

(defn node-value-type
  [x]
  ;; (prn :nvt x)
  (cond
   (api/literal? x) [(api/literal-value x) (api/datatype x) :literal x]
   (api/uri? x) [x nil :uri]
   (api/blank? x) [x nil :blank]
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

(defn compare-2
  [op a b]
  (fn [bindings]
    ;; (prn :c2 :ab a b)
    (let [[va ta] (operand-value-type bindings a)
          [vb tb] (operand-value-type bindings b)]
      ;; (prn :c2 :va va ta :vb vb tb)
      (when (and va vb)
        (cond
         (and (ns/numeric-xsd-types ta) (ns/numeric-xsd-types tb)) (op va vb)
         (= (:boolean ns/XSD) ta tb) (interpret-compare op (.compareTo ^Boolean va ^Boolean vb))
         (= (:string ns/XSD) ta tb) (interpret-compare op (.compareTo ^String va ^String vb))
         (= (:date-time ns/XSD) ta tb) (interpret-compare op (.compare ^XMLGregorianCalendar va
                                                                       ^XMLGregorianCalendar vb))
         :default nil)))))

(defn numeric-op-2
  [op a b]
  (fn [bindings]
    (let [[va ta] (operand-value-type bindings a)
          [vb tb] (operand-value-type bindings b)]
      (when (and va vb (ns/numeric-xsd-types ta) (ns/numeric-xsd-types tb))
        (op va vb)))))

(defn and*
  [& conds] #(every? (fn [f] (f %)) conds))

(defn or*
  [& conds] #(some (fn [f] (f %)) conds))

(defn exists
  [ds patterns] #(seq (q/select-join-from ds patterns % nil)))

(defn value-isa?
  [n type] #(= type (nth (operand-value-type % n) 2)))

(defn lang
  [n]
  #(let [[_ _ t x] (operand-value-type % n)] (when (= :literal t) (api/language x))))

(def compare-ops {:< < :<= <= := = :>= >= :> > :!= not=})
(def math-ops {:mul * :div / :add + :sub -})

(declare compile-filter)

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
  [q [op & args]] (apply compare-2 (compare-ops op) (compile-filter q args)))

(defmethod compile-expr :math
  [q [op & args]] (apply numeric-op-2 (math-ops op) (compile-filter q args)))

(defmethod compile-expr :and
  [q [op & args]] (apply and* (compile-filter q args)))

(defmethod compile-expr :or
  [q [op & args]] (apply or* (compile-filter q args)))

(defmethod compile-expr :exists
  [q [op & args]] (exists (:from q) (q/resolve-patterns q args)))

(defmethod compile-expr :not-exists
  [q [op & args]] #(not (exists (:from q) (q/resolve-patterns q args))))

(defmethod compile-expr :blank?
  [q [op & args]] (value-isa? (first (compile-filter q args)) :blank))

(defmethod compile-expr :uri?
  [q [op & args]] (value-isa? (first (compile-filter q args)) :uri))

(defmethod compile-expr :literal?
  [q [op & args]] (value-isa? (first (compile-filter q args)) :literal))

(defmethod compile-expr :lang
  [q [op & args]] (lang (first (compile-filter q args))))

(defmethod compile-expr :default
  [q [op & args]]
  (throw (IllegalArgumentException. (str "error compiling filter, illegal op: " op))))

(defn compile-filter
  [q spec]
  (reduce (fn [stack form] (conj stack (compile-expr q form))) [] spec))
