(defproject org.clojars.abhinav/raft-clj "0.1.0-SNAPSHOT"
  :description "A raft under construction."
  :url "https://github.com/AbhinavOmprakash/raft-clj"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :plugins [[lein-eftest "0.6.0"]]
  :dependencies [[org.clojure/clojure "1.11.1"]]
  :repl-options {:init-ns raft-clj.core})
