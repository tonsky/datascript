(ns datascript.pull-api
  (:require
   [datascript.core :as dc]
   [datascript.pull-parser :as dpp]))

(defn- into!
  [transient-coll items]
  (reduce conj! transient-coll items))

(def ^:private ^:const +default-limit+ 1000)

(defn- initial-frame
  [pattern eids multi?]
  {:state     :pattern
   :pattern   pattern
   :wildcard? (:wildcard? pattern)
   :specs     (-> pattern :attrs seq)
   :results   (transient [])
   :kvps      (transient {})
   :eids      eids
   :multi?    multi?
   :recursion {}})

(defn- subpattern-frame
  [pattern eids multi? attr]
  (assoc (initial-frame pattern eids multi?) :attr attr))

(defn- reset-frame
  [frame eids kvps]
  (let [pattern (:pattern frame)]
    (assoc frame
           :eids      eids
           :specs     (seq (:attrs pattern))
           :wildcard? (:wildcard? pattern)
           :kvps      (transient {})
           :results   (cond-> (:results frame)
                        (seq kvps) (conj! kvps)))))

(def ^:private ^:const empty-recursion-context
  {:depth 0 :seen #{}})

(def ^:private push-recursion
  (fnil
   (fn [rec eid]
     (-> rec
         (update :depth inc)
         (update :seen conj eid)))
   empty-recursion-context))

(defn- recursion-result
  [frame]
  (some-> (:kvps frame) persistent! (get ::recursion)))

(defn- recursion-frame
  [parent eid]
  (assoc (subpattern-frame (:pattern parent) [eid] false ::recursion)
         :recursion (:recursion parent)))

(defn- pull-recursion-frame
  [db [frame & frames]]
  (if-let [eids (seq (:eids frame))]
    (let [frame  (reset-frame frame (rest eids) (recursion-result frame))
          eid    (first eids)]
      (if (get-in frame [:recursion (:attr frame) :seen eid])
        (conj frames (update frame :results conj! {:db/id eid}))
        (conj frames frame (recursion-frame frame eid))))
    (let [kvps    (recursion-result frame)
          results (cond-> (:results frame)
                    (seq kvps) (conj! kvps))]
      (conj frames (assoc frame :state :done :results results)))))

(defn- recurse-attr
  [db attr multi? eids eid parent frames]
  (let [{:keys [recursion pattern]} parent
        depth  (-> recursion (get attr) (get :depth 0))]
    (if (-> pattern :attrs (get attr) :recursion (= depth))
      (conj frames parent)
      (pull-recursion-frame
       db
       (conj frames parent
             {:state :recursion :pattern pattern
              :attr attr :multi? multi? :eids eids
              :recursion (update recursion attr push-recursion eid)
              :results (transient [])})))))

(let [pattern  (dpp/PullSpec. true {})]
  (defn- expand-frame
    [attr-key multi? eids]
    (subpattern-frame pattern eids multi? attr-key)))

(defn- pull-attr-datoms
  [db attr-key attr eid forward? datoms opts [parent & frames]]
  (let [limit (get opts :limit +default-limit+)
        found (not-empty
               (cond->> datoms
                 limit (into [] (take limit))))]
    (if found
      (let [multi?     (dc/multival? db attr)
            ref?       (dc/ref? db attr)
            component? (and ref? (dc/component? db attr))
            datom-val  (if forward? #(.-v %) #(.-e %))]
        (cond
          (contains? opts :subpattern)
          (->> (subpattern-frame (:subpattern opts)
                                 (mapv datom-val found)
                                 multi? attr-key)
               (conj frames parent))

          (contains? opts :recursion)
          (recurse-attr db attr-key multi?
                        (mapv datom-val found)
                        eid parent frames)

          (and forward? component?)
          (->> found
               (mapv datom-val)
               (expand-frame attr-key multi?)
               (conj frames parent))
          
          :else 
          (let [as-value  (cond->> datom-val
                            ref? (comp #(hash-map :db/id %)))
                single?   (if forward? (not multi?) component?)]
            (->> (cond-> (into [] (map as-value) found)
                   single? first)
                 (update parent :kvps assoc! attr-key)
                 (conj frames)))))
      (->> (cond-> parent
             (contains? opts :default)
             (update :kvps assoc! attr-key (:default opts)))
           (conj frames)))))

(defn- pull-attr
  [db spec eid frames]
  (let [[attr-key opts] spec]
    (if (= :db/id attr-key)
      (if (not-empty (dc/-datoms db :eavt [eid]))
        (conj (rest frames)
              (update (first frames) :kvps assoc! :db/id eid))
        frames)
      (let [attr     (:attr opts)
            forward? (= attr-key attr)
            results  (if forward?
                       (dc/-datoms db :eavt [eid attr])
                       (dc/-datoms db :avet [attr eid]))]
        (pull-attr-datoms db attr-key attr eid forward?
                          results opts frames)))))

(defn- pull-expand-frame
  [db [frame & frames]]
  (if-let [datoms-by-attr (seq (:datoms frame))]
    (let [[attr datoms] (first datoms-by-attr)
          opts          (-> frame
                            (get-in [:pattern :attrs])
                            (get attr {}))]
      (pull-attr-datoms db attr attr (:eid frame) true datoms opts
                        (conj frames (update frame :datoms rest))))
    (let [[parent & frames] frames]
      (conj frames (update parent :kvps into! (persistent! (:kvps frame)))))))

(defn- pull-wildcard
  [db frame frames]
  (let [{:keys [eid pattern]} frame
        datoms (group-by #(.-a %) (dc/-datoms db :eavt [eid]))]
    (->> {:state :expand :kvps (transient {:db/id eid})
          :eid eid :pattern pattern :datoms (seq datoms)}
         (conj frames frame)
         (pull-expand-frame db))))

(defn- pull-pattern-frame
  [db [frame & frames]]
  (if-let [eids (seq (:eids frame))]
    (if (:wildcard? frame)
      (pull-wildcard db
                     (assoc frame
                            :specs []
                            :eid (first eids)
                            :wildcard? false)
                     frames)
      (if-let [specs (seq (:specs frame))]
        (let [spec       (first specs)
              pattern    (:pattern frame)
              new-frames (conj frames (assoc frame :specs (rest specs)))]
          (pull-attr db spec (first eids) new-frames))
        (->> frame :kvps persistent! not-empty
             (reset-frame frame (rest eids))
             (conj frames)
             (recur db))))
    (conj frames (assoc frame :state :done))))

(defn- pull-pattern
  [db frames]
  (case (:state (first frames))
    :expand     (recur db (pull-expand-frame db frames))
    :pattern    (recur db (pull-pattern-frame db frames))
    :recursion  (recur db (pull-recursion-frame db frames))
    :done       (let [[f & remaining] frames
                      result (cond-> (persistent! (:results f))
                               (not (:multi? f)) first)]
                  (if (seq remaining)
                    (->> (cond-> (first remaining)
                           result (update :kvps assoc! (:attr f) result))
                         (conj (rest remaining))
                         (recur db))
                    result))))

(defn pull-spec
  [db pattern eids multi?]
  (let [eids (into [] (map #(dc/entid-strict db %)) eids)]
    (pull-pattern db (list (initial-frame pattern eids multi?)))))

(defn pull
  [db selector eid]
  (pull-spec db (dpp/parse-pull selector) [eid] false))

(defn pull-many
  [db selector eids]
  (pull-spec db (dpp/parse-pull selector) eids true))
