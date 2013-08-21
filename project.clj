(defproject thi.ng/ldk-core "0.1.0-SNAPSHOT"
  :description "Linked Data Kit - a Clojure toolkit for the Semantic Web"
  :url "http://thi.ng/"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.clojure/data.csv "0.1.2"]
                 [org.clojure/data.json "0.2.2"]
                 [com.stuartsierra/dependency "0.1.0"]]
  :injections [(require
                '[thi.ng.ldk.core.api :as api]
                '[thi.ng.ldk.core.namespaces :as ns]
                '[thi.ng.ldk.query.core :as q]
                '[thi.ng.ldk.query.executor :as qe]
                '[thi.ng.ldk.query.expressions :as exp]
                '[thi.ng.ldk.query.resultio :as rio]
                '[thi.ng.ldk.io.core :as io]
                '[thi.ng.ldk.io.turtle :as ttl]
                '[thi.ng.ldk.inference.core :as inf]
                '[thi.ng.ldk.common.util :as u]
                '[thi.ng.ldk.store.memory :as mem]
                '[clojure.java.io :as jio])]
  :aot :all)
