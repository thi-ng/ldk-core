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

(def char-ranges
  (->> {:iri-illegal [[0x00 0x20] 0x3c 0x3e 0x22 [0x7b 0x7d] 0x5e 0x5c 0x60]
        :lang-tag [[0x41 0x5a] [0x61 0x7a] 0x2d]}
       (map (fn [[k v]] [k (apply util/interval-set v)]))
       (into {})))

(def typed-literal-patterns
  [[#"(true|false)" "xsd:boolean"]
   [#"[+-]?\d+" "xsd:integer"]
   [#"[+-]?\d*.\d+" "xsd:decimal"]
   [#"[+-]?(\d+.\d*e[+-]?\d+|.\d+e[+-]?\d+|\d+e[+-]?\d+)" "xsd:double"]])

(def pnchars-base "\u00C0-\u00D6\u00D8-\u00F6\u00F8-\u02FF\u0370-\u037D\u037F-\u1FFF\u200C-\u200D\u2070-\u218F\u2C00-\u2FEF\u3001-\uD7FF\uF900-\uFDCF\uFDF0-\uFFFD\u10000-\uEFFFF")

(def pnchars (str pnchars-base "_\\-0-9\u00B7\u0300-\u036F\u203F\u2040"))

(def uchar (str "(\\\\u([a-f0-9]{4})|\\\\u([a-f0-9]{8}))"))

(def re-patterns
  {:iri-ref (re-pattern (str "(([^\u0000-\u0020\u003c\u003e\u0022\u007b-\u007d\u005e\u005c\u0060]|" uchar ")*)>"))
   :bnode (re-pattern (str "[" pnchars-base "_0-9]([" pnchars "\\.]*[" pnchars "])?"))})

(defn trace
  [id state]
  (prn id)
  (pprint
   (map
    (fn [[k v]] [k (if (or (keyword? v) (sequential? v)) v (api/index-value v))])
    (select-keys state [:subject :predicate :object :triple-ctx :state :coll-items])))
  state)

(defn resolve-pname
  [state pname]
  (let [idx (.indexOf pname ":")]
    (when (>= idx 0)
      (when-let [prefix (get-in state [:prefixes (subs pname 0 idx)])]
        (api/make-resource (str prefix (subs pname (inc idx))))))))

(defn resolve-iri
  [state iri] (if (.startsWith iri "#") (str (:base-iri state) iri) iri))

(defn emit-curr-triple
  ([{:keys [subject predicate object] :as state}]
     ;; (prn :triple [subject predicate object])
     (update-in state [:triples] conj [subject predicate object]))
  ([state {:keys [subject predicate object]}]
     ;; (prn :triple [subject predicate object])
     (update-in state [:triples] conj [subject predicate object])))

(defn emit-triples
  [state triples]
  ;; (prn :queue triples)
  (update-in state [:triples] into triples))

(defn push-context
  [state nest-type]
  ;; (trace :push state)
  (-> state
      (update-in [:stack] #(conj (or % []) (dissoc state :stack)))
      (assoc :nest-type nest-type)))

(defn pop-context
  [state]
  (when-let [s2 (clojure.core/peek (:stack state))]
    ;; (trace :pop state)
    (assoc s2
      :stack (pop (:stack state))
      :blanks (merge (:blanks s2) (:blanks state)))))

(defn list-state? [state] (= :list (:nest-type state)))
(defn bnode-state? [state] (= :bnode (:nest-type state)))

(declare
 parse-token-doc
 parse-token-eof
 parse-token-comment
 parse-token-prefix
 parse-token-prefix-ns
 parse-token-prefix-terminal
 parse-token-iri-ref
 parse-token-bnode
 parse-token-bnode-proplist
 parse-token-predicate
 parse-token-pname
 parse-token-object
 parse-token-list
 parse-token-literal
 parse-token-literal-content
 parse-token-literal-type
 parse-token-long-string
 parse-token-lang-tag
 parse-token-end-triple?
 )

(declare spo-transitions doc-transitions obj-transitions)

(defn transition
  [state]
  (assoc state
    :state (if (list-state? state)
             parse-token-object
             (spo-transitions (:triple-ctx state)))))

(defn fail [state msg] (assoc state :error msg))

(defn skip-ws [^PushbackReader in]
  (let [c (.read in)]
    (if (Character/isWhitespace c)
      (recur in)
      (.unread in c))))

(defn peek [^PushbackReader in] (let [c (.read in)] (.unread in c) (char c)))

(defn next-char? [^PushbackReader in x] (= x (char (.read in))))

(defn read-while
  ([^PushbackReader in f unread-last?] (read-while in f unread-last? (StringBuilder.)))
  ([^PushbackReader in f unread-last? ^StringBuilder buf]
     (let [c (.read in)]
       (if (and (>= c 0) (f c))
         (recur in f unread-last? (.append buf (char c)))
         (do
           (when unread-last? (.unread in c))
           (.toString buf))))))

(defn read-until-ws
  [^PushbackReader in] (read-while in #(not (Character/isWhitespace %)) false))

(defn read-literal-or-fail
  [^PushbackReader in state lit target]
  (let [len (count lit)
        buf (make-array Character/TYPE len)
        c (.read in buf 0 len)]
    (if (< c len)
      (fail state "unexpected EOF")
      (if (= lit (apply str buf))
        (assoc state :state target)
        (fail state (str "expected '" lit "', but got '" (apply str buf) "'"))))))

(defn typed-literal?
  [x]
  (when-let [t (some (fn [[re t]] (when (re-matches re x) t)) typed-literal-patterns)]
    (api/make-literal x nil t)))

(defmulti parse-token*
  (fn [_ state]
    ;; (prn (:state state))
    (:state state)))

(defn parse-token-missing
  [^PushbackReader in state]
  (fail state (str "unimplemented state " (:state state))))

(defn parse-token-eof  [_ state] (assoc state :eof true))

(defn parse-token-doc
  [^PushbackReader in state]
  (skip-ws in)
  (let [c (peek in)
        state (-> state
                  (dissoc :subject :predicate :object :literal)
                  (assoc :triple-ctx :subject :iri-ctx :subject :comment-ok? false :comment-ctx parse-token-doc))]
    ((get doc-transitions c parse-token-pname) in state)))

(defn parse-token-prefix
  [^PushbackReader in state]
  (let [token (read-until-ws in)]
    (condp = token
      "@prefix" (parse-token-prefix-ns in state)
      "@base" (parse-token-iri-ref in (assoc state :iri-ctx :base))
      (fail state (str "illegal token following '@'" token)))))

(defn parse-token-prefix-ns
  [^PushbackReader in state]
  (skip-ws in)
  (let [[_ ns] (re-matches #"([A-Za-z0-9]+):" (read-until-ws in))]
    (if ns
      (parse-token-iri-ref in (assoc state :prefix-ns ns :iri-ctx :prefix))
      (fail state (str "illegal prefix: " _)))))

(defn parse-token-iri-ref
  [^PushbackReader in state]
  (skip-ws in)
  (if (next-char? in \<)
    (let [[_ iri] (re-matches (:iri-ref re-patterns) (read-until-ws in))]
      ;; (trace :iri state)
      (if iri
        (let [{ctx :iri-ctx prefix :prefix-ns} state
              state (condp = ctx
                      :base (assoc state :base-iri iri :state parse-token-prefix-terminal)
                      :prefix (-> state
                                  (update-in [:prefixes] conj [prefix iri])
                                  (assoc :state parse-token-prefix-terminal))
                      (-> state
                          (transition)
                          (assoc ctx (api/make-resource (resolve-iri state iri)))))]
          ((:state state) in (dissoc state :prefix-ns :iri-ctx)))
        (fail state (str "unterminated IRI-REF: " _))))
    (fail state "invalid IRI-REF")))

(defn parse-token-prefix-terminal
  [^PushbackReader in state]
  (skip-ws in)
  (let [state (read-literal-or-fail in state "." parse-token-doc)]
    (if (:error state) state ((:state state) in state))))

(defn parse-token-comment
  [^PushbackReader in state]
  (let [c (read-while in #(not (#{0x000a 0x000d} %)) false)]
    ((:comment-ctx state) in state)))

(defn parse-token-bnode
  [^PushbackReader in state]
  (.read in)
  (if (next-char? in \:)
    (let [token (read-until-ws in)
          [id] (re-matches (:bnode re-patterns) token)]
      (if id
        (let [ctx (:triple-ctx state)
              state (if (get-in state [:blanks id])
                      state
                      (assoc-in state [:blanks id] (api/make-blank-node)))
              state (-> state
                        (transition)
                        (assoc ctx (get-in state [:blanks id])))]
          ((:state state) in state))
        (fail state (str "illegal bnode label" token))))
    (fail state "illegal bnode label")))

(defn parse-token-bnode-proplist
  [^PushbackReader in {ctx :triple-ctx :as state}]
  (.read in)
  (skip-ws in)
  (let [node (api/make-blank-node)
        state (-> (if (list-state? state)
                    (update-in state [:coll-items] conj node)
                    state)
                  (transition)
                  (assoc ctx node))]
    ;; (trace :bprops state)
    (if (= \] (peek in))
      (do (.read in) state)
      (let [s2 (dissoc (push-context state :bnode) :coll-items)]
        (if (list-state? state)
          (parse-token-predicate in (assoc s2 :subject (:object state)))
          (if (= :object ctx)
            (-> s2
                (emit-curr-triple)
                (assoc :state parse-token-predicate :subject (:object state)))
            ((:state s2) in s2)))))))

(defn parse-token-predicate
  [^PushbackReader in state]
  (skip-ws in)
  (let [c (peek in)]
    (cond
     (= c \<) (parse-token-iri-ref in (assoc state :iri-ctx :predicate))
     (and (= c \#) (:comment-ok? state)) (parse-token-comment in (assoc state :comment-ctx parse-token-predicate))
     :default
     (let [c (char (.read in))]
       (if (and (= c \a) (= (peek in) \space))
         (do
           (.read in)
           ;; (prn :pname-pred :rdf:type)
           (parse-token-object in (assoc state :predicate (:type api/RDF))))
         (let [illegal (:iri-illegal char-ranges)
               pname (str c (read-while in #(not (illegal %)) true))
               nc (char (.read in))]
           ;; (prn :pname-pred pname)
           (if (= nc \space)
             (if-let [n (resolve-pname state pname)]
               (parse-token-object in (assoc state :predicate n))
               (fail state (str "unknown prefix in pname: " pname)))
             (fail state (str "unexpected char in pname: " pname " char: " nc)))))))))

(defn parse-token-pname
  [^PushbackReader in state]
  (let [illegal (:iri-illegal char-ranges)
        pname (read-while in #(not (illegal %)) true)
        nc (char (.read in))
        ctx (:triple-ctx state)]
    ;; (prn :pname pname :ctx ctx)
    (if (= nc \space)
      (if-let [n (resolve-pname state pname)]
        (let [state (-> state (transition) (assoc ctx n))]
          ((:state state) in state))
        (if (= :object ctx)
          (if-let [lit (typed-literal? pname)]
            (let [state (-> state (transition) (assoc ctx lit))]
              ((:state state) in state))
            (fail state (str "unknown prefix in object pname: " pname)))
          (fail state (str "unknown prefix in pname: " pname))))
      (fail state (str "unexpected char in pname: " nc)))))

(defn parse-token-object
  [^PushbackReader in state]
  (skip-ws in)
  (let [state (if (list-state? state)
                (if-let [obj (:object state)]
                  (update-in state [:coll-items] conj obj)
                  state)
                state)
        c (peek in)]
    ;; (trace :object state)
    (if (= \) c)
      (if (list-state? state)
        (parse-token-end-triple? in state)
        (fail state "list nesting fail"))
      ((get obj-transitions c parse-token-pname)
       in (assoc state :triple-ctx :object :iri-ctx :object)))))

(defn parse-token-list
  [^PushbackReader in {ctx :triple-ctx :as state}]
  (.read in)
  (skip-ws in)
  (let [node (api/make-blank-node)
        state (-> state (transition) (assoc ctx node))]
    ;; (trace :list state)
    (if (= \) (peek in))
      (do (.read in) (emit-triples state (api/rdf-list-triples node [])))
      (let [s2 (push-context state :list)]
        (parse-token-object in (assoc s2 :object nil :subject node :coll-items []))))))

(defn parse-token-literal
  [^PushbackReader in state]
  (let [c (char (.read in))
        n (char (.read in))
        p (peek in)
        trans (fn [q]
                (if (= n q)
                  (cond
                   (= p q) (parse-token-long-string in (assoc state :lit-terminator q))
                   (= p \@) (parse-token-lang-tag in (assoc state :literal ""))
                   (= p \^) (parse-token-literal-type in (assoc state :literal ""))
                   :default (parse-token-end-triple?
                             in (assoc state
                                  :triple-ctx :object
                                  :object (api/make-literal ""))))
                  (parse-token-literal-content in (assoc state :literal n :lit-terminator q))))]
    (cond
     (= c \") (trans \")
     (= c \') (trans \')
     :default (fail state (str "illegal literal: " c n p)))))

(defn parse-token-literal-content
  [^PushbackReader in state]
  (let [terminators #{(int (:lit-terminator state)) 0x000a 0x000d}
        lit (str (:literal state) (read-while in #(not (terminators %)) true))
        c (char (.read in))
        n (peek in)]
    (if (= c (:lit-terminator state))
      ;; TODO add regexp check
      (condp = n
        \@ (parse-token-lang-tag in (assoc state :literal lit))
        \^ (parse-token-literal-type in (assoc state :literal lit))
        (let [state (-> state (transition) (assoc :object (api/make-literal lit)))]
          ((:state state) in state)))
      (fail state "unexpected line break in literal")))) ;; TODO add xsd:string type

(defn parse-token-lang-tag
  [^PushbackReader in state]
  (.read in)
  (let [ok (:lang-tag char-ranges)
        lang (read-while in #(ok %) true)]
    (if (re-matches #"(?i)[a-z]{2,3}(-[a-z0-9]+)*" lang)
      (let [state (-> state (transition) (assoc :object (api/make-literal (:literal state) lang)))]
        ((:state state) in state))
      (fail state (str "illegal language tag: " lang)))))

(defn parse-token-end-triple?
  [^PushbackReader in state]
  (skip-ws in)
  (condp = (char (.read in))
    \] (if (= :bnode (:nest-type state))
         (let [s2 (-> state (pop-context) (emit-curr-triple state) (transition))
               s2 (if (list-state? s2) (dissoc s2 :object) s2)]
           ;; (trace :restored s2)
           s2)
         (fail state "bnode nesting fail"))
    \) (if (list-state? state)
         (let [s2 (-> state
                      (pop-context)
                      (emit-triples (api/rdf-list-triples (:subject state) (:coll-items state)))
                      (transition))]
           ;; (trace :restored s2)
           s2)
         (fail state "list nesting fail"))
    \. (-> state
           (emit-curr-triple)
           (assoc :state parse-token-doc))
    \, (-> state
           (emit-curr-triple)
           (dissoc :object :literal)
           (assoc :state parse-token-object :comment-ok? false))
    \; (-> state
           (emit-curr-triple)
           (dissoc :predicate :object :literal)
           (assoc :state parse-token-predicate :triple-ctx :predicate :comment-ok? true))
    (fail state "non-terminated triple")))

(def spo-transitions
  {:subject parse-token-predicate
   :predicate parse-token-object
   :object parse-token-end-triple?})

(def doc-transitions
  {\@ parse-token-prefix
   \# parse-token-comment
   \< parse-token-iri-ref
   \_ parse-token-bnode
   \[ parse-token-bnode-proplist
   \( parse-token-list
   \uffff parse-token-eof})

(def obj-transitions
  {\< parse-token-iri-ref
   \" parse-token-literal
   \' parse-token-literal
   \[ parse-token-bnode-proplist
   \_ parse-token-bnode
   \( parse-token-list})

(defn init-parser-state
  [& {:keys [prefixes]}]
  {:state parse-token-doc :prefixes (or prefixes {}) :blanks {}})

(defn parse-triples
  ([in]
     (parse-triples
      (PushbackReader. (io/reader in))
      (init-parser-state)))
  ([^PushbackReader in state]
     (if-let [t (first (:triples state))]
       (with-meta
         (lazy-seq
          (cons t (parse-triples in (assoc state :triples (rest (:triples state))))))
         {:prefixes (:prefixes state) :base (:base-iri state)})
       (cond
        (:error state) [state]
        (:eof state) nil
        :default (recur in ((:state state) in state))))))
