(ns raft-clj.log-test
  (:require
    [clojure.test :refer [deftest is testing]]
    [raft-clj.log :as log]))


(def term-0-entry-1 (log/map->Entry {:term 0 :command "entry 1"}))
(def term-1-entry-1 (log/map->Entry {:term 1 :command "entry 1"}))
(def term-1-entry-2 (log/map->Entry {:term 1 :command "entry 2"}))

(def term-2-entry-1 (log/map->Entry {:term 2 :command "entry 1"}))
(def term-2-entry-2 (log/map->Entry {:term 2 :command "entry 2"}))


(deftest test-create-log
  (is (= [log/dummy-entry] @(log/create))
      "New log should contain only the dummy entry and should not be empty")

  (is (= [log/dummy-entry term-1-entry-1 term-1-entry-2]
         @(log/create [term-1-entry-1 term-1-entry-2]))
      "When a log is created with entries it includes the entries and the dummy-entry")

  (is (= [log/dummy-entry]
         @(log/create [log/dummy-entry]))
      "When the entries contain a dummy-entry a dummy-entry is not created"))


(deftest test-append-entries
  ;; Success scenarios
  (testing "Can append empty entries"
    (let [log (log/create)]
      (is (= true (log/append-entries! log 0 0 [])))
      (is (= [] (log/all-entries @log)))))

  (testing "Can append entry from the same term to log"
    (let [log (log/create)]
      (is (= true (log/append-entries! log 0 0 [term-0-entry-1])))
      (is (= [term-0-entry-1] (log/all-entries @log)))))

  (testing "Can append entry from the current_term+1 to log"
    (let [log (log/create)]
      (is (= true (log/append-entries! log 0 0 [term-1-entry-1])))
      (is (= [term-1-entry-1] (log/all-entries @log)))))

  (testing "Can append entry from the any term, as long as term is greater than log's last term"
    (let [log (log/create)]
      (is (= true (log/append-entries! log 0 0 [term-2-entry-1])))
      (is (= [term-2-entry-1] (log/all-entries @log)))))

  (testing "Can append multiple entries from the same term"
    (let [log (log/create)]
      (is (= true (log/append-entries! log 0 0 [term-1-entry-1 term-1-entry-2])))
      (is (= [term-1-entry-1 term-1-entry-2] (log/all-entries @log)))))

  (testing "Can append multiple entries from the different terms as long as subsequent entries
           have equal or higher terms than previous entries"
    (let [log (log/create)]
      (is (= true (log/append-entries! log 0 0 [term-1-entry-1 term-1-entry-2 term-2-entry-1 term-2-entry-2])))
      (is (= [term-1-entry-1 term-1-entry-2 term-2-entry-1 term-2-entry-2] (log/all-entries @log)))))

  (testing "An entry with a higher term overwrites any existing entries"
    (let [log (log/create [term-1-entry-1])]
      (is (= true (log/append-entries! log 0 0 [term-2-entry-1])))
      (is (= [term-2-entry-1] (log/all-entries @log)))))

  (testing "If an existing entry conflicts with a new one
           (same index but different terms),
           delete the existing entry and all that follow it"
    (let [log (log/create [term-0-entry-1 term-1-entry-1 term-1-entry-2])]
      (is (= true (log/append-entries! log 0 0 [term-2-entry-1])))
      (is (= [term-2-entry-1] (log/all-entries @log)))))

  ;; Failure scenarios
  (testing "If appending fails due to an error the log should remain unchanged"
    (let [log (log/create)]
      (is (thrown? java.lang.AssertionError
            (log/append-entries! log 0 0 [term-2-entry-1 term-1-entry-1]))
          "Terms should be monotonically increasing in the entries. This case should never be reached")
      (is (= [] (log/all-entries @log)))))

  (testing "If appending fails due to an error the log should remain unchanged"
    (let [log (log/create)]
      (is (thrown? java.lang.AssertionError
            (log/append-entries! log -1 0 [term-1-entry-1]))
          "prev-index can't be negative")
      (is (= [] (log/all-entries @log)))))

  (testing "If appending fails due to an error the log should remain unchanged"
    (let [log (log/create)]
      (is (thrown? java.lang.AssertionError
            (log/append-entries! log 0 -1 [term-1-entry-1]))
          "prev-term can't be negative")
      (is (= [] (log/all-entries @log)))))

  (testing "Append fails if an entry is inserted after a gap"
    (let [log (log/create)]
      (is (false? (log/append-entries! log 2 0 [term-1-entry-1])))
      (is (= [] (log/all-entries @log))))
    (let [log (log/create)]
      (is (false? (log/append-entries! log 100 0 [term-1-entry-1])))
      (is (= [] (log/all-entries @log)))))

  (testing "Append fails if an entry's term is lesser than the term of last-log-entry"
    ;; create log with term-2 entry and attemp to insert term-1 entry
    (let [log (log/create [term-2-entry-1])]
      (is (false? (log/append-entries! log 2 0 [term-1-entry-1])))
      (is (= [term-2-entry-1] (log/all-entries @log))))))


(deftest test-single-threaded-append-entries-with-randomization
  (testing "Single-threaded append entries operations maintain term ordering invariants"
    (let [log (log/create)
          operations-count 500]

      (dotimes [i operations-count]
        (let [term (+ 1 (rand-int 100))  ; Random term between 1-100
              command (str "Op-" i)
              entry (log/map->Entry {:term term :command command :metadata nil})]
          (try
            (log/append-entries! log (log/index-of-last @log) (log/term-of-last-entry @log) [entry])
            (catch Exception e
              (println "Exception on operation" i ":" (.getMessage e))))))

      (let [entries (log/all-entries @log)]
        (is (log/monotonically-increasing? :term entries)
            "After sequential operations, terms in log should be monotonically increasing")))))


(deftest test-concurrent-append-entries-with-randomization
  (testing "Concurrent append entries operations maintain term ordering invariants"
    (dotimes [_ 100]
      (let [log (log/create)
            thread-count 5
            operations-per-thread 100
            latch (java.util.concurrent.CountDownLatch. thread-count)
            start-signal (java.util.concurrent.CountDownLatch. 1)]

        (dotimes [t thread-count]
          (future
            (.await start-signal)
            (try
              (dotimes [i operations-per-thread]
                (let [term-of-last-entry (log/term-of-last-entry @log)
                      term (rand-int (+ term-of-last-entry 100))  ; Random term between 1-100
                      command (str "Thread-" t "-Op-" i)
                      entry (log/map->Entry {:term term :command command :metadata nil})]
                  (try
                    (log/append-entries! log (log/index-of-last @log) term-of-last-entry [entry])
                    (catch Exception e
                      nil))))
              (finally
                (.countDown latch)))))

        (.countDown start-signal)

        (.await latch)

        (let [entries (log/all-entries @log)]
          (is (log/monotonically-increasing? :term entries)
              "After concurrent operations, terms in log should be monotonically increasing"))))))
