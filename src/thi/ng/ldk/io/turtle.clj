(ns thi.ng.ldk.io.turtle
  (:refer-clojure :exclude [peek])
  (:require
   [thi.ng.ldk.core
    [api :as api]
    [namespaces :as ns :refer [default-namespaces]]]
   [thi.ng.ldk.common.util :as util]
   [clojure
    [string :as str]
    [pprint :refer [pprint]]]
   [clojure.java.io :as io])
  (:import
   [java.io PushbackReader]
   [java.util.regex Pattern MatchResult]))

(def char-ranges
  (->> {:iri-illegal [[0x00 0x20] 0x3c 0x3e 0x22 [0x7b 0x7d] 0x5e 0x5c 0x60]
        :lang-tag [[0x41 0x5a] [0x61 0x7a] 0x2d]}
       (map (fn [[k v]] [k (apply util/interval-set v)]))
       (into {})))

(def typed-literal-patterns
  [[#"(true|false)" (:boolean ns/XSD)]
   [#"[+-]?\d+" (:integer ns/XSD)]
   [#"[+-]?\d*.\d+" (:decimal ns/XSD)]
   [#"[+-]?(\d+.\d*e[+-]?\d+|.\d+e[+-]?\d+|\d+e[+-]?\d+)" (:double ns/XSD)]])

(def pn-chars-base "A-Za-z\u00C0-\u00D6\u00D8-\u00F6\u00F8-\u02FF\u0370-\u037D\u037F-\u1FFF\u200C-\u200D\u2070-\u218F\u2C00-\u2FEF\u3001-\uD7FF\uF900-\uFDCF\uFDF0-\uFFFD\u10000-\uEFFFF")

(def pn-chars-u (str pn-chars-base "_"))

(def pn-chars (str pn-chars-u "\\-\\\\d\u00B7\u0300-\u036F\u203F\u2040"))

(def hex (str "[A-Fa-f0-9]"))

(def pl-local-esc "\\\\[_~\\.\\-!$&'\\(\\)\\*+,;=/\\?#@%]")

(def plx (str "((%" hex hex ")|(" pl-local-esc "))"))

(def pn-prefix (str "([" pn-chars-base "]([" pn-chars "\\.]*[" pn-chars "])?)"))

(def pn-local (str "(([" pn-chars-u "\\:\\d])|" plx ")((([" pn-chars "\\.\\:])|" plx ")*(([" pn-chars "\\:])|" plx "))?"))

(def pname-ns (str "(" pn-prefix "?):"))

(def pname-ln (str pname-ns pn-local))

(def uchar (str "(?:\\\\u([a-f0-9]{4}))|(?:\\\\u([a-f0-9]{8}))"))

(def echar (str "(?:\\\\[tbnrf\"'\\\\])"))

(def re-patterns
  {:iri-ref #"<((?:[^\x00-\x20<>\"\{\}\|\^\`]|\\[uU])*)>"
   :bnode (re-pattern (str "[" pn-chars-base "_0-9]([" pn-chars "\\.]*[" pn-chars "])?"))
   :escape #"\\u([a-fA-F0-9]{4})|\\U([a-fA-F0-9]{8})|\\[uU]|\\(.)"
   :pname (re-pattern (str pname-ln "|" pname-ns))
   :pname-ns (re-pattern pname-ns)
   :lit-chars-double (re-pattern (str "((?:[^\"\\\\\\u000a\\u000d])|" uchar "|" echar ")*"))
   :lit-chars-single (re-pattern (str "((?:[^'\\\\\\u000a\\u000d])|" uchar "|" echar ")*"))
   :lit-chars-long-dbl (re-pattern (str "((?:[^\"\\\\])|" uchar "|" echar ")*"))
   :lit-chars-long-sgl (re-pattern (str "((?:[^'\\\\])|" uchar "|" echar ")*"))
   })

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
  ;; (prn id)
  (pprint
   (map
    (fn [[k v]] [k (if (or (keyword? v) (sequential? v)) v (api/index-value v))])
    (select-keys state [:subject :predicate :object :triple-ctx :state :coll-items])))
  state)

(defn resolve-iri
  [state ^String iri] (if (neg? (.indexOf iri ":")) (str (:base-iri state) iri) iri))

(defn emit-curr-triple
  ([{:keys [subject predicate object triples] :as state}]
     ;; (prn :triple [subject predicate object])
     (if (and subject predicate object)
       (assoc state
         :triples (conj triples [subject predicate object])
         :triple-complete? true)
       state))
  ([state {:keys [subject predicate object]}]
     ;; (prn :triple [subject predicate object])
     (if (and subject predicate object)
       (assoc state
         :triples (conj (:triples state) [subject predicate object])
         :triple-complete? true)
       state)))

(defn emit-triples
  [state triple-coll]
  (assoc state :triples (concat (:triples state) triple-coll)))

(defn add-prefix
  [state prefix iri]
  (when-let [h (get-in state [:handlers :on-prefix])] (h prefix iri))
  (assoc state :prefixes (assoc (:prefixes state) prefix iri)))

(defn set-base-iri
  [state iri]
  (when-let [h (get-in state [:handlers :on-base])] (h iri))
  (assoc state :base-iri iri))

(defn push-context
  [state nest-type]
  ;; (trace :push state)
  ;; (prn :push)
  (-> state
      (assoc :stack (conj (get state :stack []) (dissoc state :stack)))
      (assoc :nest-type nest-type)))

(defn pop-context
  [state]
  (when-let [s2 (clojure.core/peek (:stack state))]
    ;; (trace :pop state)
    ;; (prn :pop)
    (assoc s2
      :stack (pop (:stack state))
      :blanks (merge (:blanks s2) (:blanks state)))))

(defn list-state? [state] (= :list (:nest-type state)))
(defn bnode-state? [state] (= :bnode (:nest-type state)))

(declare
 parse-doc
 parse-eof
 parse-comment
 parse-prefix
 parse-prefix-ns
 parse-prefix-terminal
 parse-iri-ref
 parse-bnode
 parse-bnode-proplist
 parse-predicate
 parse-pname
 parse-object
 parse-list
 parse-literal
 parse-literal-content
 parse-literal-type
 parse-long-string
 parse-lang-tag
 parse-end-triple)

(declare spo-transitions doc-transitions obj-transitions)

(defn transition
  [state]
  (assoc state
    :state (if (list-state? state)
             parse-object
             (spo-transitions (:triple-ctx state)))))

(defn fail [state msg] (assoc state :error msg))

(defn skip-ws [^PushbackReader in]
  (let [c (.read in)]
    (if (Character/isWhitespace c)
      (recur in)
      (.unread in c))))

(defn peek [^PushbackReader in] (let [c (.read in)] (.unread in c) (char c)))

(defn next-char? [^PushbackReader in x] (= (char x) (char (.read in))))

(defn remove-comment
  [^String s]
  (let [idx (.indexOf s "#")] (if (>= idx 0) (subs s 0 idx) s)))

(defn read-while
  ([^PushbackReader in f unread-last?] (read-while in f unread-last? (StringBuilder.)))
  ([^PushbackReader in f unread-last? ^StringBuilder buf]
     (loop [c (.read in) buf buf]
       (if (and (>= c 0) (f c))
         (recur (.read in) (.append buf (char c)))
         (do
           (when unread-last? (.unread in c))
           (.toString buf))))))

(defn read-until-ws
  [^PushbackReader in] (read-while in #(not (Character/isWhitespace (int %))) true))

(defn read-until-linebreak
  [^PushbackReader in] (read-while in #(not (or (= 0x0a %) (= 0x0d %))) false))

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

(defn read-iri-ref
  [^PushbackReader in state]
  (let [src (read-until-ws in)
        [_ iri] (re-matches (:iri-ref re-patterns) src)
        iri (unescape iri)]
    (if iri
      [iri state src]
      [nil (fail state (str "invalid IRI-REF: " src)) src])))

(defn pname-without-comment
  [^PushbackReader in ^String pname]
  (read-until-linebreak in)
  (subs pname 0 (.indexOf pname "#")))

(defn read-pname
  ([^PushbackReader in state] (read-pname in state nil))
  ([^PushbackReader in state prepend]
     (let [src (str prepend (read-until-ws in))
           [pname] (re-matches (:pname re-patterns) src)
           pname (if (and (nil? pname) (pos? (.indexOf src "#")))
                   (pname-without-comment in src)
                   pname)
           pname (ns/resolve-pname (:prefixes state) (unescape pname))]
       ;; (prn :pname pname :src src)
       (if pname
         [(api/make-resource pname) state src]
         [nil (fail state (str "invalid pname: " src)) src]))))

(defn typed-literal  [x]
  (when-let [t (some (fn [[re t]] (when (re-matches re x) t)) typed-literal-patterns)]
    (api/make-literal x nil t)))

(defn parse-eof [_ state] (assoc state :eof true))

(defn parse-doc
  [^PushbackReader in state]
  ;; (prn :doc)
  (skip-ws in)
  ((get doc-transitions (peek in) parse-pname) in
   (assoc state
     :triple-ctx :subject :iri-ctx :subject
     :triple-complete? false
     :comment-ok? false :comment-ctx parse-doc)))

(defn parse-prefix
  [^PushbackReader in state]
  ;; (prn :prefix)
  (let [token (read-until-ws in)]
    (condp = token
      "@prefix" (parse-prefix-ns in (assoc state :sparql-prefix false))
      "@base" (parse-iri-ref in (assoc state :iri-ctx :base :sparql-prefix false))
      (fail state (str "illegal token following '@'" token)))))

(defn parse-prefix-ns
  [^PushbackReader in state]
  ;; (prn :prefix-ns)
  (skip-ws in)
  (let [src (read-until-ws in)
        [_ ns] (re-matches (:pname-ns re-patterns) src)]
    (if ns
      (parse-iri-ref in (assoc state :prefix-ns ns :iri-ctx :prefix))
      (fail state (str "illegal prefix: " src)))))

(defn parse-iri-ref
  [^PushbackReader in state]
  ;; (prn :iri-ref)
  (skip-ws in)
  (let [[iri state] (read-iri-ref in state)]
    (if iri
      (let [{ctx :iri-ctx prefix :prefix-ns} state]
        (condp = ctx
          :base (parse-prefix-terminal in (set-base-iri state iri))
          :prefix (parse-prefix-terminal in (add-prefix state prefix iri))
          (-> state
              (assoc ctx (api/make-resource (resolve-iri state iri)))
              (dissoc :prefix-ns :iri-ctx)
              (transition))))
      state)))

(defn parse-prefix-terminal
  [^PushbackReader in state]
  ;; (prn :terminal (:sparql-prefix state))
  (skip-ws in)
  (if (:sparql-prefix state)
    (-> state (dissoc :prefix-ns :iri-ctx) (assoc :state parse-doc))
    (if (next-char? in \.)
      (-> state (dissoc :prefix-ns :iri-ctx) (assoc :state parse-doc))
      (fail state (str "unterminated prefix or base declaration: " (:prefix-ns state))))))

(defn parse-comment
  [^PushbackReader in state]
  ;; (prn :comment :ctx (:comment-ctx state))
  (let [c (read-while in #(not (#{0x000a 0x000d} %)) false)]
    ((:comment-ctx state) in state)))

(defn parse-bnode
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
        (fail state (str "illegal bnode label: " token))))
    (fail state "illegal bnode label")))

(defn parse-bnode-proplist
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
          (parse-predicate in (assoc s2 :subject (:object state)))
          (if (= :object ctx)
            (-> s2
                (emit-curr-triple)
                (assoc :state parse-predicate :subject (:object state)))
            ((:state s2) in s2)))))))

(defn parse-predicate
  [^PushbackReader in state]
  ;; (prn :predicate)
  (skip-ws in)
  (let [c (peek in)]
    (cond
     (= c \.) (if (:triple-complete? state)
                (do (.read in) (parse-doc in state))
                (fail "illegal statement terminator at predicate position"))
     (= c \<) (parse-iri-ref in (assoc state :iri-ctx :predicate :triple-ctx :predicate))
     (= c \]) (parse-end-triple in state)
     (and (= c \#) (:comment-ok? state)) (parse-comment in (assoc state :comment-ctx parse-predicate))
     :default
     (let [c (char (.read in))]
       ;; (prn :c c)
       (if (and (= c \a) (= (peek in) \space))
         (parse-object in (assoc state :predicate (:type api/RDF) :triple-ctx :object))
         (let [[pname state src] (read-pname in state c)]
           ;; (prn :pname-pred pname src (:error state))
           (if pname
             (parse-object in (assoc state :predicate pname :triple-ctx :object))
             state)))))))

(defn parse-pname
  [^PushbackReader in state]
  ;; (prn :pname)
  (let [[pname state src] (read-pname in state)
        ctx (:triple-ctx state)]
    ;; (prn :pname pname src :ctx ctx)
    (if pname
      (let [state (-> state (transition) (assoc ctx pname))]
        ((:state state) in state))
      (condp = ctx
        :subject (condp = src
                   "PREFIX" (parse-prefix-ns
                             in (-> state (dissoc :error) (assoc :sparql-prefix true)))
                   "BASE" (parse-iri-ref
                           in (-> state (dissoc :error) (assoc :iri-ctx :base :sparql-prefix true)))
                   state)
        :object (if-let [lit (typed-literal src)]
                  (let [state (-> state (transition) (assoc ctx lit) (dissoc :error))]
                    ((:state state) in state))
                  state)
        state))))

(defn parse-object
  [^PushbackReader in state]
  ;; (prn :object (list-state? state) (:object state))
  (skip-ws in)
  (let [state (if (list-state? state)
                (if-let [obj (:object state)]
                  (-> state (update-in [:coll-items] conj obj) (dissoc :object))
                  state)
                state)
        c (peek in)]
    ;; (trace :object state)
    (if (= \) c)
      (if (list-state? state)
        (parse-end-triple in state)
        (fail state "list nesting fail"))
      ((get obj-transitions c parse-pname)
       in (assoc state :triple-ctx :object :iri-ctx :object :comment-ctx parse-object)))))

(defn parse-list
  [^PushbackReader in {ctx :triple-ctx :as state}]
  ;; (prn :list)
  (.read in)
  (skip-ws in)
  (let [node (api/make-blank-node)
        state (-> state (transition) (assoc ctx node))]
    ;; (trace :list state)
    (if (= \) (peek in))
      (do (.read in)
          (emit-triples state (api/rdf-list-triples node [])))
      (parse-object
       in (-> state
              (push-context :list)
              (assoc :object nil :subject node :coll-items []))))))

(defn parse-literal
  [^PushbackReader in state]
  ;; (prn :literal)
  (let [c (char (.read in))
        n (char (.read in))
        p (peek in)
        trans (fn [q re]
                (if (= n (char q))
                  (cond
                   (= p q) (parse-long-string in (assoc state :lit-terminator (str q q q)))
                   (= p \@) (parse-lang-tag in (assoc state :literal ""))
                   (= p \^) (parse-literal-type in (assoc state :literal ""))
                   :default (parse-end-triple
                             in (assoc state
                                  :triple-ctx :object
                                  :object (api/make-literal ""))))
                  (parse-literal-content in (assoc state :literal n :lit-terminator q :lit-regex re))))]
    (cond
     (= c \") (trans \" :lit-chars-double)
     (= c \') (trans \' :lit-chars-single)
     :default (fail state (str "illegal literal: " c n p)))))

(defn parse-literal-content
  [^PushbackReader in state]
  ;; (prn :lit-content)
  (let [src (str (:literal state) (read-until-literal in (str (:lit-terminator state))))
        [lit] (re-matches ((:lit-regex state) re-patterns) src)
        lit (unescape lit)
        n (peek in)]
    (if lit
      (condp = n
        \@ (parse-lang-tag in (assoc state :literal lit))
        \^ (parse-literal-type in (assoc state :literal lit))
        (let [state (-> state (transition) (assoc :object (api/make-literal lit)))]
          ((:state state) in state)))
      (fail state (str "illegal escape seq in literal: " src))))) ;; TODO add xsd:string type

(defn parse-long-string
  [^PushbackReader in state]
  ;; (prn :long-string)
  (.read in)
  (if-let [lit (read-until-literal in (:lit-terminator state))]
    (if-let [lit (unescape lit)]
      (condp = (peek in)
        \@ (parse-lang-tag in (assoc state :literal lit))
        \^ (parse-literal-type in (assoc state :literal lit))
        (let [state (-> state (transition) (assoc :object (api/make-literal lit)))]
          ((:state state) in state)))
      (fail state (str "illegal escape sequence in literal: " lit)))
    (fail state "unterminated literal")))

(defn parse-lang-tag
  [^PushbackReader in state]
  ;; (prn :lang)
  (.read in)
  (let [ok (:lang-tag char-ranges)
        lang (read-while in #(ok %) true)]
    (if (re-matches #"(?i)[a-z]{2,3}(-[a-z0-9]+)*" lang)
      (let [state (-> state (transition) (assoc :object (api/make-literal (:literal state) lang)))]
        ((:state state) in state))
      (fail state (str "illegal language tag: " lang)))))

(defn parse-literal-type
  [^PushbackReader in state]
  ;; (prn :lit-type)
  (.read in)
  (if (next-char? in \^)
    (let [[iri state] (if (= (peek in) \<) (read-iri-ref in state) (read-pname in state))]
      (if iri
        (let [state (-> state (transition) (assoc :object (api/make-literal (:literal state) nil (api/label iri))))]
          ((:state state) in state))
        state))))

(defn parse-end-triple
  [^PushbackReader in state]
  ;; (prn :end-triple)
  (skip-ws in)
  (condp = (char (.read in))
    \] (if (bnode-state? state)
         (let [s2 (-> state (pop-context) (emit-curr-triple state))]
           (if (= :subject (:triple-ctx s2))
             (do
               (skip-ws in)
               (if (= \. (peek in))
                 (do (.read in) (assoc s2 :state parse-doc))
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
           (dissoc :subject :predicate :object :literal)
           (assoc :state parse-doc))
    \, (-> state
           (emit-curr-triple)
           (dissoc :object :literal)
           (assoc :state parse-object :comment-ok? false))
    \; (-> state
           (emit-curr-triple)
           (dissoc :predicate :object :literal)
           (assoc :state parse-predicate :triple-ctx :predicate :comment-ok? true))
    (fail state "non-terminated triple")))

(def spo-transitions
  {:subject parse-predicate
   :predicate parse-object
   :object parse-end-triple})

(def doc-transitions
  {\@ parse-prefix
   \# parse-comment
   \< parse-iri-ref
   \_ parse-bnode
   \[ parse-bnode-proplist
   \( parse-list
   \uffff parse-eof})

(def obj-transitions
  {\< parse-iri-ref
   \" parse-literal
   \' parse-literal
   \[ parse-bnode-proplist
   \_ parse-bnode
   \( parse-list
   \# parse-comment})

(defn init-reader
  [in] (PushbackReader. (io/reader in)))

(defn init-parser-state
  [& {:keys [prefixes base on-prefix on-base on-triple on-complete on-error meta?]}]
  {:state parse-doc
   :prefixes (or prefixes default-namespaces)
   :blanks {}
   :base-iri base
   :handlers {:on-prefix on-prefix
              :on-base on-base
              :on-triple on-triple
              :on-complete on-complete
              :on-error on-error}
   :meta? meta?})

(defn parse-triples
  ([in]
     (parse-triples (init-reader in) (init-parser-state)))
  ([^PushbackReader in state]
     (if-let [t (first (:triples state))]
       (lazy-seq
        (cons t (parse-triples in (assoc state :triples (rest (:triples state))))))
       (cond
        (:error state) [state]
        (:eof state) nil
        :default (recur in ((:state state) in state))))))

(defn parse-triples-with-meta
  ([in]
     (parse-triples-with-meta (init-reader in) (init-parser-state)))
  ([^PushbackReader in state]
     (if-let [t (first (:triples state))]
       (lazy-seq
        (cons
         (with-meta t {:prefixes (:prefixes state) :base (:base-iri state)})
         (parse-triples-with-meta in (assoc state :triples (rest (:triples state))))))
       (cond
        (:error state) [state]
        (:eof state) nil
        :default (recur in ((:state state) in state))))))

(defn parse-triples-with-handlers
  ([in]
     (parse-triples-with-handlers (init-reader in) (init-parser-state)))
  ([^PushbackReader in state]
     (if-let [t (first (:triples state))]
       (let [t (if-let [h (get-in state [:handlers :on-triple])] (h t) t)
             state (assoc state :triples (rest (:triples state)))]
         (if t
           (lazy-seq (cons t (parse-triples-with-handlers in state)))
           (recur in state)))
       (cond
        (:error state) (if-let [h (get-in state [:handlers :on-error])] (h state) [state])
        (:eof state) (when-let [h (get-in state [:handlers :on-complete])] (h state))
        :default (recur in ((:state state) in state))))))
