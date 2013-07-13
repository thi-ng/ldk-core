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
   [java.io PushbackReader]
   [java.util.regex Pattern MatchResult]))

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

(def pn-chars-base "A-Za-z\u00C0-\u00D6\u00D8-\u00F6\u00F8-\u02FF\u0370-\u037D\u037F-\u1FFF\u200C-\u200D\u2070-\u218F\u2C00-\u2FEF\u3001-\uD7FF\uF900-\uFDCF\uFDF0-\uFFFD\u10000-\uEFFFF")

(def pn-chars-u (str pn-chars-base "_"))

(def pn-chars (str pn-chars-u "\\-\\\\d\u00B7\u0300-\u036F\u203F\u2040"))

(def hex (str "[A-Fa-f0-9]"))

(def pl-local-esc "\\\\[_~\\.\\-!$&'\\(\\)\\*+,;=/\\?#@%]")

(def plx (str "((%" hex hex ")|(" pl-local-esc "))"))

(def pn-prefix (str "([" pn-chars-base "]([" pn-chars "\\.]*[" pn-chars "])?)"))

(def pn-local (str "(([" pn-chars-u "\\:\\d])|" plx ")((([" pn-chars "\\.\\:])|" plx ")*(([" pn-chars "\\:])|" plx "))?"))

(def pname-ns (str pn-prefix "?:"))

(def pname-ln (str pname-ns pn-local))

(def uchar (str "(?:\\\\u([a-f0-9]{4}))|(?:\\\\u([a-f0-9]{8}))"))

(def re-patterns
  {:iri-ref #"<((?:[^\x00-\x20<>\"\{\}\|\^\`]|\\[uU])*)>"
   :bnode (re-pattern (str "[" pn-chars-base "_0-9]([" pn-chars "\\.]*[" pn-chars "])?"))
   :escape #"\\u([a-fA-F0-9]{4})|\\U([a-fA-F0-9]{8})|\\[uU]|\\(.)"
   :pname (re-pattern (str pname-ln "|" pname-ns))})

(def esc-replacements
  {"\\" \\ "'" \' "\"" \"
   "n" "\n" "r" "\r" "t" "\t" "f" "\f" "b" "\b"
   "_" \_ "~" \~ "." \. "-" \- "!" \! "$" \$ "&" \&
   "(" \( ")" \) "*" \* "+" \+ "," \, ";" \; "=" \=
   "/" \/ "?" \? "#" \# "@" \@ "%" \%})

(defn re-replace
  [^Pattern re ^String s f]
  (loop [m (re-matcher re s) b (StringBuilder.) idx 0]
    (if (.find m)
      (let [mr (.toMatchResult m)]
        (.append b (subs s idx (.start mr)))
        (when-let [s (f mr)] (recur m (.append b s) (.end mr))))
      (.toString (.append b (subs s idx))))))

(defn unescape
  [^String s]
  (when s
    (re-replace
     (:escape re-patterns) s
     (fn [^MatchResult mr]
       (let [u4 (.group mr 1) u8 (.group mr 2) uc (.group mr 3)]
         (cond
          u4 (char (Integer/parseInt u4 16))
          u8 (let [c (Long/parseLong u8 16)]
               (if (< c 0xefff)
                 (char c)
                 (str (char (+ 0xd800 (/ (- c 0x10000) 0x400)))
                      (char (+ 0xdc00 (rem (- c 0x10000) 0x400))))))
          uc (esc-replacements uc)
          :default nil))))))

(defn trace
  [id state]
  (prn id)
  (pprint
   (map
    (fn [[k v]] [k (if (or (keyword? v) (sequential? v)) v (api/index-value v))])
    (select-keys state [:subject :predicate :object :triple-ctx :state :coll-items])))
  state)

(defn resolve-pname
  [state ^String pname]
  (when pname
    (let [idx (.indexOf pname ":")]
      (when (>= idx 0)
        (when-let [prefix (get-in state [:prefixes (subs pname 0 idx)])]
          (api/make-resource (str prefix (subs pname (inc idx)))))))))

(defn resolve-iri
  [state ^String iri] (if (.startsWith iri "#") (str (:base-iri state) iri) iri))

(defn emit-curr-triple
  ([{:keys [subject predicate object triples] :as state}]
     ;; (prn :triple [subject predicate object])
     (assoc state :triples (conj triples [subject predicate object])))
  ([{:keys [triples] :as state} {:keys [subject predicate object]}]
     ;; (prn :triple [subject predicate object])
     (assoc state :triples (conj triples [subject predicate object]))))

(defn emit-triples
  [{:keys [triples] :as state} triple-coll]
  ;; (prn :queue triples)
  (assoc state :triples (into triples triple-coll)))

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
  (let [state (assoc state
                :state (if (list-state? state)
                         parse-token-object
                         (spo-transitions (:triple-ctx state))))]
    ;; (prn :transition (dissoc state :coll-items :stack :prefixes :blanks))
    state))

(defn fail [state msg] (assoc state :error msg))

(defn skip-ws [^PushbackReader in]
  (let [c (.read in)]
    (if (Character/isWhitespace c)
      (recur in)
      (.unread in c))))

(defn peek [^PushbackReader in] (let [c (.read in)] (.unread in c) (char c)))

(defn next-char? [^PushbackReader in x] (= (char x) (char (.read in))))

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
  [^PushbackReader in] (read-while in #(not (Character/isWhitespace (int %))) false))

(defn read-until-literal
  [^PushbackReader in lit]
  (let [len (count lit)
        l1 (inc len)]
    ;; (prn :read-until lit)
    (loop [buf (StringBuilder.) bl 1]
      (let [c (.read in)]
        (when (>= c 0)
          (.append buf (char c))
          (cond
           (> bl len) (let [s (.substring buf (- bl l1))]
                        (if (and (not (.startsWith s "\\")) (= (subs s 1) lit))
                          (.substring buf 0 (- bl len))
                          (recur buf (inc bl))))
           (= bl len) (if (= (.substring buf (- bl len)) lit)
                        (.substring buf 0 (- bl len))
                        (recur buf (inc bl)))
           :default (recur buf (inc bl))))))))

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

(defn read-iri-ref
  [^PushbackReader in state]
  (let [src (read-until-ws in)
        [_ iri] (re-matches (:iri-ref re-patterns) src)
        iri (unescape iri)]
    (if iri
      [iri state src]
      [nil (fail state (str "invalid IRI-REF: " src)) src])))

(defn read-pname
  ([^PushbackReader in state] (read-pname in state nil))
  ([^PushbackReader in state prepend]
     (let [src (str prepend (read-until-ws in))
           [pname] (re-matches (:pname re-patterns) src)
           uname (unescape pname)
           rname (resolve-pname state uname)]
       ;; (prn :read-pname rname uname src)
       (if rname
         [rname state src]
         [nil (fail state (str "invalid pname: " src)) src]))))

(defn typed-literal  [x]
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
  ;; (prn :doc)
  (skip-ws in)
  (let [c (peek in)
        state (-> state
                  (dissoc :subject :predicate :object :literal)
                  (assoc :triple-ctx :subject :iri-ctx :subject :comment-ok? false :comment-ctx parse-token-doc))]
    ((get doc-transitions c parse-token-pname) in state)))

(defn parse-token-prefix
  [^PushbackReader in state]
  ;; (prn :prefix)
  (let [token (read-until-ws in)]
    (condp = token
      "@prefix" (parse-token-prefix-ns in state)
      "@base" (parse-token-iri-ref in (assoc state :iri-ctx :base))
      (fail state (str "illegal token following '@'" token)))))

(defn parse-token-prefix-ns
  [^PushbackReader in state]
  ;; (prn :prefix-ns)
  (skip-ws in)
  (let [[_ ns] (re-matches #"([A-Za-z0-9]*):" (read-until-ws in))]
    (if ns
      (parse-token-iri-ref in (assoc state :prefix-ns ns :iri-ctx :prefix))
      (fail state (str "illegal prefix: " _)))))

(defn parse-token-iri-ref
  [^PushbackReader in state]
  ;; (prn :iri-ref)
  (skip-ws in)
  (let [[iri state] (read-iri-ref in state)]
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
      state)))

(defn parse-token-prefix-terminal
  [^PushbackReader in state]
  ;; (prn :terminal)
  (skip-ws in)
  (let [state (read-literal-or-fail in state "." parse-token-doc)]
    (if (:error state) state ((:state state) in state))))

(defn parse-token-comment
  [^PushbackReader in state]
  (let [c (read-while in #(not (#{0x000a 0x000d} %)) false)]
    ((:comment-ctx state) in state)))

(defn parse-token-bnode
  [^PushbackReader in state]
  ;; (prn :comment)
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
  ;; (prn :bnode-proplist)
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
  ;; (prn :predicate)
  (skip-ws in)
  (let [c (peek in)]
    (cond
     (= c \<) (parse-token-iri-ref in (assoc state :iri-ctx :predicate))
     (and (= c \#) (:comment-ok? state)) (parse-token-comment in (assoc state :comment-ctx parse-token-predicate))
     :default
     (let [c (char (.read in))]
       ;; (prn :c c)
       (if (and (= c \a) (= (peek in) \space))
         (parse-token-object in (assoc state :predicate (:type api/RDF) :triple-ctx :object))
         (let [[pname state src] (read-pname in state c)]
           ;; (prn :pname-pred pname src (:error state))
           (if pname
             (parse-token-object in (assoc state :predicate pname :triple-ctx :object))
             state)))))))

(defn parse-token-pname
  [^PushbackReader in state]
  ;; (prn :pname)
  (let [[pname state src] (read-pname in state)
        ctx (:triple-ctx state)]
    ;;(prn :pname pname src :ctx ctx)
    (if pname
      (let [state (-> state (transition) (assoc ctx pname))]
        ((:state state) in state))
      (if (= :object ctx)
        (if-let [lit (typed-literal src)]
          (let [state (-> state (transition) (assoc ctx lit) (dissoc :error))]
            ((:state state) in state))
          state)
        state))))

(defn parse-token-object
  [^PushbackReader in state]
  ;; (prn :object)
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
  ;; (prn :list)
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
  ;; (prn :literal)
  (let [c (char (.read in))
        n (char (.read in))
        p (peek in)
        trans (fn [q]
                (if (= n (char q))
                  (cond
                   (= p q) (parse-token-long-string in (assoc state :lit-terminator (str q q q)))
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
  ;; (prn :lit-content)
  (let [terminators #{(int (:lit-terminator state)) 0x000a 0x000d}
        lit (str (:literal state) (read-while in #(not (terminators %)) true))
        c (char (.read in))
        n (peek in)]
    (if (= c (char (:lit-terminator state)))
      ;; TODO add regexp check
      (condp = n
        \@ (parse-token-lang-tag in (assoc state :literal lit))
        \^ (parse-token-literal-type in (assoc state :literal lit))
        (let [state (-> state (transition) (assoc :object (api/make-literal lit)))]
          ((:state state) in state)))
      (fail state "unexpected line break in literal")))) ;; TODO add xsd:string type

(defn parse-token-long-string
  [^PushbackReader in state]
  ;; (prn :long-string)
  (.read in)
  (if-let [lit (read-until-literal in (:lit-terminator state))]
    (if-let [lit (unescape lit)]
      (condp = (peek in)
        \@ (parse-token-lang-tag in (assoc state :literal lit))
        \^ (parse-token-literal-type in (assoc state :literal lit))
        (let [state (-> state (transition) (assoc :object (api/make-literal lit)))]
          ((:state state) in state)))
      (fail state (str "illegal escape sequence in literal: " lit)))
    (fail state "unterminated literal")))

(defn parse-token-lang-tag
  [^PushbackReader in state]
  ;; (prn :lang)
  (.read in)
  (let [ok (:lang-tag char-ranges)
        lang (read-while in #(ok %) true)]
    (if (re-matches #"(?i)[a-z]{2,3}(-[a-z0-9]+)*" lang)
      (let [state (-> state (transition) (assoc :object (api/make-literal (:literal state) lang)))]
        ((:state state) in state))
      (fail state (str "illegal language tag: " lang)))))

(defn parse-token-literal-type
  [^PushbackReader in state]
  ;; (prn :lit-type)
  (.read in)
  (if (next-char? in \^)
    (let [[iri state] (if (= (peek in) \<) (read-iri-ref in state) (read-pname in state))]
      (if iri
        (let [state (-> state (transition) (assoc :object (api/make-literal (:literal state) nil iri)))]
          ((:state state) in state))
        state))))

(defn parse-token-end-triple?
  [^PushbackReader in state]
  ;; (prn :end-triple)
  (skip-ws in)
  (condp = (char (.read in))
    \] (if (= :bnode (:nest-type state))
         (let [s2 (-> state (pop-context) (emit-curr-triple state))]
           (if (= :subject (:triple-ctx s2))
             (do
               (skip-ws in)
               (if (= \. (peek in))
                 (do (.read in) (assoc s2 :state parse-token-doc))
                 (transition s2)))
             (if (list-state? s2) (dissoc s2 :object) s2)))
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
     ;; (prn (:state state) :err (:error state))
     (if-let [t (first (:triples state))]
       (lazy-seq
        (cons t (parse-triples in (assoc state :triples (rest (:triples state))))))
       (cond
        (:error state) [state]
        (:eof state) nil
        :default (recur in ((:state state) in state))))))
