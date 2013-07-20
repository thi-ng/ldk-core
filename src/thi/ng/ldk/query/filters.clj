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
  (prn :op op :r r)
  (cond
   (neg? r) (or (= < op) (= <= op))
   (zero? r) (= = op)
   :default (or (= > op) (= >= op))))

(defn compare-2
  [op a b]
  (fn [bindings]
    (let [[va ta] (operand-value-type bindings a)
          [vb tb] (operand-value-type bindings b)]
      (prn :v va vb)
      (prn :t ta tb)
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

(defn not-exists
  [ds patterns] #(not (seq (q/select-join-from ds patterns % nil))))

(def compare-ops {:< < :<= <= := = :>= >= :> >})
(def math-ops {:mul * :div / :add + :sub -})

(defn compile-filter
  [q spec]
  (reduce
   (fn [stack form]
     ;; (prn form stack)
     (if (sequential? form)
       (let [[op & more] form
             f (cond
                (compare-ops op) (apply compare-2 (compare-ops op) (compile-filter q more))
                (math-ops op) (apply numeric-op-2 (math-ops op) (compile-filter more))
                (= :and op) (apply and* (compile-filter q more))
                (= :or op) (apply or* (compile-filter q more))
                (= :not-exists op) (not-exists (:from q) (q/resolve-patterns q more))
                :default (throw (IllegalArgumentException. (str "error compiling filter, illegal op: " op))))]
         (when f (conj stack f)))
       (conj stack form)))
   [] spec))
