(ns thi.ng.triplestore.ttl
  (:refer-clojure :exclude [peek])
  (:require
   [thi.ng.triplestore.util :as util]
   [thi.ng.triplestore.api :as api]
   [clojure.string :as str]
   [clojure.java.io :as io])
  (:import
   [java.io PushbackReader]))

(defn trace
  [id state]
  (prn id (map (fn [[k v]] [k (if (keyword? v) v (api/index-value v))])
               (select-keys state [:subject :predicate :object :triple-ctx]))))

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
  (let [[ns local :as pn] (str/split pname #":")
        prefix (if (str/blank? ns)
                 (:base-ns state)
                 (get-in state [:ns-map ns]))]
    (when (and prefix (= 2 (count pn)))
      (api/make-resource (str prefix local)))))

(defn get-triple
  [{:keys [subject predicate object]}]
  (prn :triple [subject predicate object])
  [subject predicate object])

(defn skip-ws [^PushbackReader in]
  (let [c (.read in)]
    (if (Character/isWhitespace c)
      (recur in) (.unread in c))))

(defn peek [^PushbackReader in] (let [c (.read in)] (.unread in c) (char c)))

(defn next-char? [^PushbackReader in x] (= x (char (.read in))))

(defn push-context
  [state]
  (trace :push state)
  (-> state
      (update-in [:stack] #(conj (or % []) (dissoc state :stack)))
      (assoc :bnode? true)))

(defn pop-context
  [state]
  (when-let [s2 (clojure.core/peek (:stack state))]
    (trace :pop state)
    (assoc s2 :stack (pop (:stack state)))))

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
      \# (assoc state :state :comment)
      \_ (assoc state :state :bnode :triple-ctx :subject)
      \[ (assoc state :state :bnode-proplist :triple-ctx :subject)
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
      (if (next-char? in \>)
        (let [{ctx :iri-ctx prefix :prefix-ns} state
              state (dissoc state :prefix-ns :iri-ctx)]
          (condp = ctx
            :base (assoc state :base-ns iri :state :terminal)
            :prefix (-> state
                        (update-in [:ns-map] conj [prefix iri])
                        (assoc :state :terminal))
            (assoc state ctx (api/make-resource iri) :state (spo-transitions ctx))))
        (error state (str "unterminated IRI: " iri))))))

(defmethod read-token :terminal
  [^PushbackReader in state]
  (skip-ws in)
  (read-literal in state "." :doc))

(defmethod read-token :comment
  [^PushbackReader in state]
  (let [c (read-while in #(not (#{0x000a 0x000d} %)) false)]
    (assoc state :state :doc)))

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
          (assoc state
            ctx (get-in state [:blanks id])
            :state (spo-transitions ctx)))))
    (error state "illegal bnode label")))

(defmethod read-token :bnode-proplist
  [^PushbackReader in {ctx :triple-ctx :as state}]
  (.read in)
  (skip-ws in)
  (let [state (assoc state
                ctx (api/make-blank-node)
                :state (spo-transitions ctx))]
    (trace :bprops state)
    (if (= \] (peek in))
      (do (.read in) state)
      (let [s2 (push-context state)]
        (if (= :object ctx)
          (assoc s2
            :triple (get-triple s2)
            :state :predicate
            :subject (:object state))
          s2)))))

(defmethod read-token :predicate
  [^PushbackReader in state]
  (skip-ws in)
  (let [c (peek in)]
    (if (= c \<)
      (assoc state :state :iri-ref :iri-ctx :predicate)
      (assoc state :state :pname-pred))))

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
    (prn :pname pname)
    (if (= nc \space)
      (if-let [n (resolve-pname state pname)]
        (assoc state ctx n :state (spo-transitions ctx))
        (error state (str "unknown prefix in pname: " pname)))
      (error state (str "unexpected char in pname: " nc)))))

(defmethod read-token :object
  [^PushbackReader in state]
  (skip-ws in)
  (let [c (peek in)]
    (condp = c
      \< (assoc state :state :iri-ref :iri-ctx :object)
      \" (assoc state :state :literal)
      \' (assoc state :state :literal)
      \[ (assoc state :state :bnode-proplist :triple-ctx :object) ;; TODO push stack?
      \_ (assoc state :state :bnode :triple-ctx :object)
      \( (assoc state :state :coll)
      (assoc state :state :pname :triple-ctx :object))))

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
      (assoc state
        :state :end-triple?
        :object (api/make-literal lit))))) ;; TODO add xsd:string type

(defmethod read-token :lang-tag
  [^PushbackReader in state]
  (.read in)
  (let [ok (:lang-tag char-ranges)
        lang (read-while in #(ok %) true)]
    (assoc state
      :state :end-triple?
      :object (api/make-literal (:literal state) lang)))) ;; TODO add xsd:string type

(defmethod read-token :end-triple?
  [^PushbackReader in state]
  (skip-ws in)
  (condp = (char (.read in))
    \] (if (:bnode? state)
         (let [s2 (pop-context state)
               s2 (assoc s2
                    :triple (get-triple state)
                    :state (spo-transitions (:triple-ctx s2)))]
           (trace :restored s2)
           s2)
         (error state "nesting error"))
    \. (-> state
           (dissoc :subject :predicate :object :literal)
           (assoc :triple (get-triple state) :state :doc))
    \, (-> state
           (dissoc :object :literal)
           (assoc :triple (get-triple state) :state :object))
    \; (-> state
           (dissoc :predicate :object :literal)
           (assoc :triple (get-triple state) :state :predicate))
    (error state "non-terminated triple")))

(defn parse-triples
  ([in]
     (parse-triples
      (PushbackReader. (io/reader in))
      {:state :doc :ns-map {} :blanks {}}))
  ([^PushbackReader in state]
     (if (:triple state)
       (with-meta
         (lazy-seq
          (cons (:triple state) (parse-ttl in (dissoc state :triple))))
         {:prefixes (:ns-map state)})
       (cond
        (:error state) [state]
        (= :eof (:state state)) nil
        :default (recur in (read-token in state))))))
