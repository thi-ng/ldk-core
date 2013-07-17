(ns thi.ng.ldk.test.query.core
  (:require
   [thi.ng.ldk.core.api :as api]
   [thi.ng.ldk.query.core :as q]
   [thi.ng.ldk.store.memory :as mem])
  (:use clojure.test))

(defn make-model
  [] (api/update-model (mem/make-dataset) :inf (mem/make-store)))

(deftest
  (testing "FIXME, I fail."
    (is (= 0 1))))
