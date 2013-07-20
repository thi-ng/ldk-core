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
  (cond
   (api/literal? x) [(api/literal-value x) (api/datatype x)]
   (api/uri? x) [x :uri]
   (api/blank? x) [x :blank]
   :default nil))

(defn operand-value-type
  [bindings x]
  (cond
   (q/qvar? x) (when-let [v (get bindings x)] (node-value-type v))
   (satisfies? api/PNode x) (node-value-type x)
   (fn? x) (recur bindings (x bindings))
   :default  [x (api/xsd-type x)]))

(defn interpret-compare
  [op r]
  (cond
   (neg? r) (or (= < op) (= <= op))
   (zero? r) (= = op)
   :default (or (= > op) (= >= op))))

(defn compare-2
  [op a b]
  (fn [bindings]
    (let [[va ta] (operand-value-type bindings a)
          [vb tb] (operand-value-type bindings b)]
      (when (and va vb)
        (cond
         (and (ns/numeric-xsd-types ta) (ns/numeric-xsd-types tb)) (op va vb)
         (= (:boolean ns/XSD) ta tb) (op va vb)
         (= (:string ns/XSD) ta tb) (interpret-compare op (.compare ^String va ^String vb))
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

(def compare-ops {:< < :<= <= := = :>= >= :> > :!= not=})
(def math-ops {:mul * :div / :add + :sub -})

(defmulti compile-expr
  (fn [q form]
    (if (sequential? form)
      (let [op (first form)]
        (cond
         (compare-ops op) :compare
         (math-ops op) :math
         :default op))
      :atom)))

(defmethod compile-expr :atom [q a] a)

(defmethod compile-expr :compare
  [q [op & args]]
  (apply compare-2 (compare-ops op) (compile-filter q args)))

(defmethod compile-expr :math
  [q [op & args]]
  (apply numeric-op-2 (math-ops op) (compile-filter q args)))

(defmethod compile-expr :and
  [q [op & args]]
  (apply and* (compile-filter q args)))

(defmethod compile-expr :or
  [q [op & args]]
  (apply or* (compile-filter q args)))

(defmethod compile-expr :exists
  [q [op & args]]
  #(exists (:from q) (q/resolve-patterns q args)))

(defmethod compile-expr :not-exists
  [q [op & args]]
  #(not (exists (:from q) (q/resolve-patterns q args))))

(defmethod compile-expr :default
  [q [op & args]]
  (throw (IllegalArgumentException. (str "error compiling filter, illegal op: " op))))

(defn compile-filter
  [q spec]
  (reduce (fn [stack form] (conj stack (compile-expr q form))) [] spec))
