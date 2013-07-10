(ns thi.ng.triplestore.ttl
  (:refer-clojure :exclude [peek])
  (:require
   [thi.ng.triplestore
    [api :as api]
    [namespaces :refer [*default-ns-map*]]
    [util :as util]]
   [clojure.string :as str]
   [clojure.java.io :as io]
   [clojure.pprint :refer [pprint]])
  (:import
   [java.io PushbackReader]))

(defn trace
  [id state]
  (prn id)
  (pprint
   (map
    (fn [[k v]] [k (if (or (keyword? v) (sequential? v)) v (api/index-value v))])
    (select-keys state [:subject :predicate :object :triple-ctx :state :coll-items])))
  state)

(defn error
  [state msg] (assoc state :error msg))

(def char-ranges
  (->> {:iri-illegal [[0x00 0x20] 0x3c 0x3e 0x22 [0x7b 0x7d] 0x5e 0x5c 0x60]
        :lang-tag [[0x41 0x5a] [0x61 0x7a] 0x2d]}
       (map (fn [[k v]] [k (apply util/interval-set v)]))
       (into {})))

(def spo-transitions
  {:subject :predicate :predicate :object :object :end-triple?})

(defn resolve-pname
  [state pname]
  (let [idx (.indexOf pname ":")]
    (when (>= idx 0)
      (when-let [prefix (get-in state [:prefixes (subs pname 0 idx)])]
        (api/make-resource (str prefix (subs pname (inc idx))))))))

(defn emit-triple
  ([{:keys [subject predicate object] :as state}]
     (prn :triple [subject predicate object])
     (update-in state [:triples] conj [subject predicate object]))
  ([state {:keys [subject predicate object]}]
     (prn :triple [subject predicate object])
     (update-in state [:triples] conj [subject predicate object])))

(defn queue-triples
  [state triples]
  (prn :queue triples)
  (update-in state [:triples] into triples))

(defn skip-ws [^PushbackReader in]
  (let [c (.read in)]
    (if (Character/isWhitespace c)
      (recur in) (.unread in c))))

(defn peek [^PushbackReader in] (let [c (.read in)] (.unread in c) (char c)))

(defn next-char? [^PushbackReader in x] (= x (char (.read in))))

(defn push-context
  [state nest-type]
  (trace :push state)
  (-> state
      (update-in [:stack] #(conj (or % []) (dissoc state :stack)))
      (assoc :nest-type nest-type)))

(defn pop-context
  [state]
  (when-let [s2 (clojure.core/peek (:stack state))]
    (trace :pop state)
    (-> s2
        (assoc :stack (pop (:stack state)))
        (assoc :blanks (merge (:blanks s2) (:blanks state))))))

(defn list-state? [state] (= :list (:nest-type state)))
(defn bnode-state? [state] (= :bnode (:nest-type state)))

(defn transition
  [state]
  (assoc state :state (if (list-state? state) :object (spo-transitions (:triple-ctx state)))))

(defn read-while
  ([^PushbackReader in f unread-last?] (read-while in f unread-last? (StringBuffer.)))
  ([^PushbackReader in f unread-last? ^StringBuffer buf]
     (let [c (.read in)]
       (if (and (not (neg? c)) (f c))
         (recur in f unread-last? (.append buf (char c)))
         (do
           (when unread-last? (.unread in c))
           (.toString buf))))))

(defn read-literal
  [^PushbackReader in state lit target]
  (let [len (count lit)
        buf (make-array Character/TYPE len)
        c (.read in buf 0 len)]
    (if (< c len)
      (assoc state :error "unexpected EOF")
      (if (= lit (apply str buf))
        (assoc state :state target)
        (error state (str "expected '" lit "', but got '" (apply str buf) "'"))))))

(defmulti read-token
  (fn [_ state]
    (prn (:state state))
    (:state state)))

(defmethod read-token :default
  [^PushbackReader in state]
  (error state (str "unimplemented state " (:state state))))

(defmethod read-token :doc
  [^PushbackReader in state]
  (skip-ws in)
  (let [c (peek in)]
    (condp = c
      \@ (assoc state :state :prefix)
      \# (assoc state :state :comment :comment-ctx :doc)
      \_ (assoc state :state :bnode :triple-ctx :subject)
      \[ (assoc state :state :bnode-proplist :triple-ctx :subject)
      \( (assoc state :state :list :triple-ctx :subject)
      \uffff (assoc state :state :eof)
      (assoc state :state :pname :triple-ctx :subject))))

(defmethod read-token :prefix
  [^PushbackReader in state]
  (.read in)
  (condp = (char (.read in))
    \p (read-literal in state "refix" :prefix-ns)
    \b (read-literal in (assoc state :iri-ctx :base) "ase" :iri-ref)))

(defmethod read-token :prefix-ns
  [^PushbackReader in state]
  (skip-ws in)
  (let [ns (read-while in #(not= 0x003a %) false)]
    ;; TODO add re-check
    (assoc state :prefix-ns ns :state :iri-ref :iri-ctx :prefix)))

(defmethod read-token :iri-ref
  [^PushbackReader in state]
  (skip-ws in)
  (if (next-char? in \<)
    (let [illegal (:iri-illegal char-ranges)
          iri (read-while in #(not (illegal %)) true)]
      (trace :iri state)
      (if (next-char? in \>)
        (let [{ctx :iri-ctx prefix :prefix-ns} state
              state (dissoc state :prefix-ns :iri-ctx)]
          (condp = ctx
            :base (assoc state :base-ns iri :state :terminal)
            :prefix (-> state
                        (update-in [:prefixes] conj [prefix iri])
                        (assoc :state :terminal))
            (-> state
                (transition)
                (assoc ctx (api/make-resource
                            (if (.startsWith iri "#")
                              (str (:base-ns state) iri)
                              iri))))))
        (error state (str "unterminated IRI: " iri))))))

(defmethod read-token :terminal
  [^PushbackReader in state]
  (skip-ws in)
  (read-literal in state "." :doc))

(defmethod read-token :comment
  [^PushbackReader in state]
  (let [c (read-while in #(not (#{0x000a 0x000d} %)) false)]
    (assoc state :state (:comment-ctx state))))

(defmethod read-token :bnode
  [^PushbackReader in state]
  (.read in)
  (if (next-char? in \:)
    (let [illegal (:iri-illegal char-ranges)
          id (read-while in #(not (illegal %)) true)]
      (if (next-char? in \space)
        (let [state (if (get-in state [:blanks id]) state
                        (assoc-in state [:blanks id] (api/make-blank-node)))
              ctx (:triple-ctx state)]
          (prn :blanks (:blanks state))
          (prn :blank id (get-in state [:blanks id]))
          (-> state (transition) (assoc ctx (get-in state [:blanks id]))))
        (error state "illegal character after bnode")))
    (error state "illegal bnode label")))

(defmethod read-token :bnode-proplist
  [^PushbackReader in {ctx :triple-ctx :as state}]
  (.read in)
  (skip-ws in)
  (let [node (api/make-blank-node)
        state (-> state (transition) (assoc ctx node))
        state (if (list-state? state)
                (update-in state [:coll-items] conj node)
                state)]
    (trace :bprops state)
    (if (= \] (peek in))
      (do (.read in) state)
      (let [s2 (dissoc (push-context state :bnode) :coll-items)]
        (if (list-state? state)
          (assoc s2 :state :predicate :subject (:object state))
          (if (= :object ctx)
            (-> s2
                (emit-triple)
                (assoc :state :predicate :subject (:object state)))
            s2))))))

(defmethod read-token :predicate
  [^PushbackReader in state]
  (skip-ws in)
  (let [c (peek in)]
    (cond
     (= c \<) (assoc state :state :iri-ref :iri-ctx :predicate)
     (and (= c \#) (:comment-ok? state)) (assoc state :state :comment :comment-ctx :predicate)
     :default (assoc state :state :pname-pred))))

(defmethod read-token :pname-pred
  [^PushbackReader in state]
  (let [c (char (.read in))]
    (if (and (= c \a) (= (peek in) \space))
      (do
        (.read in)
        (prn :pname-pred :rdf:type)
        (assoc state :predicate (:type api/RDF) :state :object))
      (let [illegal (:iri-illegal char-ranges)
            pname (str c (read-while in #(not (illegal %)) true))
            nc (char (.read in))]
        (prn :pname-pred pname)
        (if (= nc \space)
          (if-let [n (resolve-pname state pname)]
            (assoc state :predicate n :state :object)
            (error state (str "unknown prefix in pname: " pname)))
          (error state (str "unexpected char in pname: " pname " char: " nc)))))))

(defmethod read-token :pname
  [^PushbackReader in state]
  (let [illegal (:iri-illegal char-ranges)
        pname (read-while in #(not (illegal %)) true)
        nc (char (.read in))
        ctx (:triple-ctx state)]
    (prn :pname pname :ctx ctx)
    (if (= nc \space)
      (if-let [n (resolve-pname state pname)]
        (-> state (transition) (assoc ctx n))
        (error state (str "unknown prefix in pname: " pname)))
      (error state (str "unexpected char in pname: " nc)))))

(defmethod read-token :object
  [^PushbackReader in state]
  (skip-ws in)
  (let [state (if (list-state? state)
                (if-let [obj (:object state)]
                  (update-in state [:coll-items] conj obj)
                  state)
                state)
        c (peek in)]
    (trace :object state)
    (condp = c
      \< (assoc state :state :iri-ref :iri-ctx :object)
      \" (assoc state :state :literal :triple-ctx :object)
      \' (assoc state :state :literal :triple-ctx :object)
      \[ (assoc state :state :bnode-proplist :triple-ctx :object) ;; TODO push stack?
      \_ (assoc state :state :bnode :triple-ctx :object)
      \( (assoc state :state :list :triple-ctx :object)
      \) (if (list-state? state)
           (assoc state :state :end-triple?)
           (error state "list nesting error"))
      (assoc state :state :pname :triple-ctx :object))))

(defmethod read-token :list
  [^PushbackReader in {ctx :triple-ctx :as state}]
  (.read in)
  (skip-ws in)
  (let [node (api/make-blank-node)
        state (-> state (transition) (assoc ctx node))]
    (trace :list state)
    (if (= \) (peek in))
      (do (.read in) (queue-triples state (api/rdf-list-triples node [])))
      (let [s2 (push-context state :list)]
        (trace :coll-obj-s2 (assoc s2 :state :object :object nil :subject node :coll-items []))
        ))))

(defmethod read-token :literal
  [^PushbackReader in state]
  (let [c (char (.read in))
        n (char (.read in))
        p (peek in)
        check (fn [q]
                (cond
                 (= n q) (cond
                          (= p q) (assoc state :state :long-string :lit-terminator q)
                          (= p \@) (assoc state :state :lang-tag :literal "")
                          (= p \^) (assoc state :state :literal-type :literal "")
                          :default (assoc state
                                     :state :end-triple?
                                     :triple-ctx :object
                                     :object (api/make-literal ""))) ;; TODO add xsd:string type
                 :default (assoc state
                            :state :literal-content :literal n :lit-terminator q)))]
    (cond
     (= c \") (check \")
     (= c \') (check \')
     :default (error state (str "illegal literal: " c n p)))))

(defmethod read-token :literal-content
  [^PushbackReader in state]
  (let [terminators #{(int (:lit-terminator state)) 0x000a 0x000d}
        lit (str (:literal state) (read-while in #(not (terminators %)) false))
        c (peek in)]
    (condp = c
      \@ (assoc state :state :lang-tag :literal lit)
      \^ (assoc state :state :literal-type :literal lit)
      (-> state (transition) (assoc :object (api/make-literal lit)))))) ;; TODO add xsd:string type

(defmethod read-token :lang-tag
  [^PushbackReader in state]
  (.read in)
  (let [ok (:lang-tag char-ranges)
        lang (read-while in #(ok %) true)]
    (if (re-matches #"(?i)[a-z]{2,3}(-[a-z0-9]+)*" lang)
      (-> state (transition) (assoc :object (api/make-literal (:literal state) lang)))
      (error state (str "illegal language tag: " lang)))))

(defmethod read-token :end-triple?
  [^PushbackReader in state]
  (skip-ws in)
  (condp = (char (.read in))
    \] (if (= :bnode (:nest-type state))
         (let [s2 (-> state (pop-context) (emit-triple state) (transition))
               s2 (if (list-state? s2) (dissoc s2 :object) s2)]
           (trace :restored s2)
           s2)
         (error state "bnode nesting error"))
    \) (if (list-state? state)
         (let [s2 (-> state
                      (pop-context)
                      (queue-triples (api/rdf-list-triples (:subject state) (:coll-items state)))
                      (transition))]
           (trace :restored s2)
           s2)
         (error state "list nesting error"))
    \. (-> state
           (emit-triple)
           (dissoc :subject :predicate :object :literal)
           (assoc :state :doc :comment-ok? false))
    \, (-> state
           (emit-triple)
           (dissoc :object :literal)
           (assoc :state :object :comment-ok? false))
    \; (-> state
           (emit-triple)
           (dissoc :predicate :object :literal)
           (assoc :state :predicate :triple-ctx :predicate :comment-ok? true))
    (error state "non-terminated triple")))

(defn init-parser-state
  [& {:keys [prefixes]}]
  {:state :doc :prefixes (or prefixes {}) :blanks {}})

(defn parse-triples
  ([in]
     (parse-triples
      (PushbackReader. (io/reader in))
      (init-parser-state)))
  ([^PushbackReader in state]
     (if-let [t (first (:triples state))]
       (with-meta
         (lazy-seq
          (cons t (parse-triples in (update-in state [:triples] rest))))
         {:prefixes (:prefixes state)})
       (cond
        (:error state) [state]
        (= :eof (:state state)) nil
        :default (recur in (read-token in state))))))
