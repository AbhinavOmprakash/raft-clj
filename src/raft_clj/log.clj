(ns raft-clj.log)


(defrecord Entry
  [term command metadata])


(def dummy-entry (map->Entry {:term 0 :command "" :metadata ::dummy-entry}))


(defn create
  "Returns an atom of a vector with entries."
  ([] (create []))
  ([entries]
   ;; check if first entry is a dummy entry
   (let [entries' (if (= (:metadata (first entries)) ::dummy-entry)
                    entries
                    (into [dummy-entry] entries))]
     (atom (vec entries')))))


;; All these functions assume log is a vector and not an atom.
;; I envision the usaged to be something like (swap! log append-entries ...) or (number-of-entries @log)
;; the caller has the burden. this way it is easier to write tests or so I predict
(defn number-of-entries
  [log]
  (->> log
       count
       ;; decrement to account for dummy entry
       dec))


(defn all-entries
  [log]
  (->> log
       rest))


(defn index-of-last
  [log]
  ;; Raft Logs are 1-indexed so the number-of-entries = index of last item
  ;; [dummy, entry-1, entry-2]
  ;; here number-of-entries is 2 (dummy is not counted) which is also the index of the last item
  (number-of-entries log))


(defn term-of-last-entry
  [log]
  (-> log
      (nth (index-of-last log))
      :term))


(defn add-new-command
  "Used by leaders to append to their log.
  Bypasses any checking of term and other logic that is used in `append-entries`.

  `command`: string
  "
  [log term command]
  {:pre [(vector? log)
         (string? command)]
   ;; is this check useful?
   :post [(fn [updated-log]
            (= (inc (count log)) updated-log))]}
  (assert (<= (term-of-last-entry log) term)
          "Attempting to insert a log entry with term lesser than `term-log-last-entry`.
          This would violate the constraint that entries are in monotonically increasing order")
  (conj log (map->Entry {:term term
                         :command command})))


(defn monotonically-increasing?
  [k entries]
  (or (< (count entries) 2)
      (loop [prev-term (k (first entries))
             remaining (rest entries)]
        (if (empty? remaining)
          true
          (let [current-term (k (first remaining))]
            (if (< current-term prev-term)
              false
              (recur current-term (rest remaining))))))))


(defn append-entries!
  "Implementation of AppendEntries, side-effectful,
  modifies log with `swap!`and returns a boolean

  `log`: is an atom
  `prev-index`: int
  `prev-term`:int
  `entries`:vector of `Entry`s

  IDK if the paper ever explicitly indicates whether append is atomic or not,
  I have chosen to make it atomic by ensuring all entries are valid entries that can be inserted.
  This prevents a case list this-
    Attempt inserting [term-2-entry-1 term-1-entry-1]
    where term-2-entry-1 can be inserted but
    the insert fails at `term-1-entry-1` because
  `term-1-entry-1`'s term is less than `term-2-entry-1`
  leaving the log in an inconsistent state."
  [log prev-index prev-term entries]
  {:pre [(instance? clojure.lang.Atom log)
         (int? prev-index)
         (not (neg? prev-index))
         (int? prev-term)
         (not (neg? prev-term))
         (vector? entries)
         ;; FYI: In theory this should never fail because of that invariant that
         ;; the log can only contain entries with monotonically increasing terms
         ;; but it doesn't hurt to be defensive here
         (monotonically-increasing? :term entries)]
   :post [(boolean? %)]}
  (cond
    ;; If prev-index is greater than index-of-last item then that means there are holes in the log
    (> prev-index (index-of-last @log)) false

    ;; RAFT paper condition 2 Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
    (not= (:term (get @log prev-index)) prev-term) false

    :else
    (loop [prev-index' prev-index
           entries' entries]
      ;; append-entries should succeed when leader tries to append empty entries
      (if (empty? entries')
        true
        (let [index-to-insert-at (inc prev-index')
              entry-to-insert (first entries')
              existing-entry (get @log index-to-insert-at)]
          (if-not existing-entry
            ;; simple case
            (do (swap! log assoc index-to-insert-at entry-to-insert)
                (recur index-to-insert-at (rest entries')))
            ;; tricky case
            (cond
              (= existing-entry entry-to-insert)
              ;; don't insert or append, continue the looping
              (do (swap! log assoc index-to-insert-at entry-to-insert)
                  (recur index-to-insert-at (rest entries')))

              ;; if the term of the existing-entry is less than or equal to the leaders term
              ;; then the leader's entry is the most up-to-date one so, overwrite the current entry
              ;; and delete all existing entries that follow it
              (<= (:term existing-entry) (:term entry-to-insert))
              (do
                ;; ; delete existing entries
                (swap! log subvec 0 index-to-insert-at)
                ;; insert new entry
                (swap! log assoc index-to-insert-at entry-to-insert)
                (recur index-to-insert-at (rest entries')))

              ;; if the term of the existing-entry is greater than the leader's term
              ;; that means the leader is outdated and hence the append should fail
              ;; this case can happen when there is a network partition and a leader gets separated from the pack and then returns
              ;; thinking that it is still the leader.
              :else false)))))))
