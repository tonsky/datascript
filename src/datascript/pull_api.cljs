(ns datascript.pull-api
  (:require
   [datascript.core :as dc]
   [datascript.pull-parser :as pp]))

(defprotocol IPullExprType
  (-expr-type [this]))

(extend-protocol IPullExprType
  pp/PullAttrName
  (-expr-type [this] :attr)
  pp/PullDefaultExpr
  (-expr-type [this] :default)
  pp/PullLimitExpr
  (-expr-type [this] :limit)
  pp/PullMapSpecEntry
  (-expr-type [this] :map-entry)
  pp/PullPattern
  (-expr-type [this] :pattern)
  pp/PullRecursionLimit
  (-expr-type [this] :recursion)
  pp/PullReverseAttrName
  (-expr-type [this] :attr-reverse)
  pp/PullWildcard
  (-expr-type [this] :wildcard))

(defn- into!
  [transient-coll items]
  (reduce conj! transient-coll items))

(defn- initial-frame
  [pattern eids multi?]
  {:state     :pattern
   :pattern   pattern
   :specs     (.-specs pattern)
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
  (assoc frame
         :eids    eids
         :specs   (get-in frame [:pattern :specs])
         :kvps    (transient {})
         :results (cond-> (:results frame)
                    (seq kvps) (conj! kvps))))

(def ^:private ^:const +default-limit+ 1000)

(let [pattern  (pp/PullPattern. [(pp/PullWildcard. #{})])]
  (defn- expand-frame
    [attr-key multi? eids]
    (subpattern-frame pattern eids multi? attr-key)))

(defn- pull-attr-datoms
  [db attr-key attr forward? datoms frame [parent & frames]]
  (let [{:keys [limit expand?] :or {expand? true}} frame
        found (not-empty
                 (cond->> datoms
                   limit (into [] (take limit))))]
    (if found
      (let [multi?     (dc/multival? db attr)
            ref?       (dc/ref? db attr)
            component? (and ref? (dc/component? db attr))]
        (if (and expand? forward? component?)
          (->> found
               (into [] (map #(.-v %)))
               (expand-frame attr-key multi?)
               (conj frames parent))
          (let [datom-val (if forward? #(.-v %) #(.-e %))
                as-value  (if ref?
                            #(hash-map :db/id (datom-val %))
                            #(datom-val %))
                single?   (if forward? (not multi?) component?)]
            (->> (cond-> (into [] (map as-value) found)
                   single? first)
                 (update parent :kvps assoc! attr-key)
                 (conj frames)))))
      (->> (cond-> parent
             (contains? frame :default)
             (update :kvps assoc! attr-key (:default frame)))
           (conj frames)))))

(defn- pull-attr-frame
  [db [frame & frames]]
  (let [{:keys [spec eid]} frame
        attr    (.-attr spec)]
    (if (= :db/id attr)
      (if (not-empty (dc/-datoms db :eavt [eid]))
        (conj (rest frames)
              (update (first frames) :kvps assoc! attr eid))
        frames)
      (let [results (dc/-datoms db :eavt [eid attr])]
        (pull-attr-datoms db attr attr true results frame frames)))))

(defn- pull-attr-reverse-frame
  [db [frame & frames]]
  (let [{:keys [spec eid]} frame
        attr    (.-attr spec)
        results (dc/-datoms db :avet [attr eid])
        frame   (assoc frame :expand? false)]
    (pull-attr-datoms db (.-rattr spec) attr false results frame frames)))

(defn- pull-default-frame
  [db [frame & frames]]
  (let [spec  (:spec frame)
        attr  (.-attr spec)
        etype (-expr-type attr)]
    (->> (assoc frame :state etype :spec attr :default (.-value spec))
         (conj frames))))

(defn- pull-expand-frame
  [db [frame & frames]]
  (if-let [datoms-by-attr (seq (:datoms frame))]
    (let [[attr datoms] (first datoms-by-attr)]
      (pull-attr-datoms db attr attr true datoms
                        {:limit +default-limit+ :expand? true}
                        (conj frames (update frame :datoms rest))))
    (let [[parent & frames] frames]
      (conj frames (update parent :kvps into! (persistent! (:kvps frame)))))))

(defn- pull-limit-frame
  [db [frame & frames]]
  (let [spec  (:spec frame)
        attr  (.-attr spec)
        etype (-expr-type attr)]
    (conj frames (assoc frame :state etype :spec attr :limit (.-limit spec)))))

(defn- pull-map-entry-key-frame
  [db [frame & frames]]
  (let [{:keys [spec eid pattern]} frame
        attr      (.-attr spec)
        etype     (-expr-type attr)
        frame     (assoc frame :state :map-entry-val :kvps (transient {}))
        key-frame {:state etype :pattern pattern :spec attr :eid eid
                   :expand? false :limit +default-limit+}]
    (conj frames frame key-frame)))

(defn- pull-map-entry-val-frame
  [db [frame & frames]]
  (let [spec      (:spec frame)
        porrl     (.-porrl spec)
        key-res   (-> frame :kvps persistent! seq)]
    (if key-res
      (let [[attr vs] (first key-res)
            frame     (assoc frame :state :map-entry-res :kvps (transient {}))
            multi?    (not (map? vs))
            eids      (if multi? (into [] (map :db/id) vs) [(:db/id vs)])
            val-frame (case (-expr-type porrl)
                        :pattern   (subpattern-frame porrl eids multi? attr)
                        :recursion {:state :recursion :pattern (:pattern frame)
                                    :attr attr :spec porrl :multi? multi?
                                    :eids eids :eid (:eid frame)
                                    :recursion (:recursion (first frames))})]
        (conj frames frame val-frame))
      frames)))

(defn- pull-map-entry-res-frame
  [db [frame parent & frames]]
  (->> (persistent! (:kvps frame))
       (update parent :kvps into!)
       (conj frames)))

(defn- pull-pattern-frame
  [db [frame & frames]]
  (if-let [eids (seq (:eids frame))]
    (if-let [specs (seq (:specs frame))]
      (let [spec    (first specs)
            etype   (-expr-type spec)
            pattern (:pattern frame)]
        (conj frames
              (assoc frame :specs (rest specs))
              {:state etype :pattern pattern :spec spec
               :eid (first eids) :limit +default-limit+}))
      (->> frame :kvps persistent! not-empty
           (reset-frame frame (rest eids))
           (conj frames)))
    (conj frames (assoc frame :state :done))))

(def ^:private ^:const empty-recursion-context
  {:depth 0 :seen #{}})

(def ^:private push-recursion
  (fnil
   (fn [rec eid]
     (-> rec
         (update :depth inc)
         (update :seen conj eid)))
   empty-recursion-context))

(defn- pull-recursion-frame
  [db [frame & frames]]
  (let [attr  (:attr frame)
        depth (get-in frame [:recursion attr :depth] 0)]
    (if (-> frame :spec .-limit (= depth))
      frames
      (conj frames
            (-> frame
                (assoc :state :recursion-res :results (transient []))
                (update-in [:recursion attr] push-recursion (:eid frame)))))))

(defn- recursion-result
  [frame]
  (some-> (:kvps frame) persistent! (get ::recursion)))

(defn- recursion-frame
  [parent eid]
  (assoc (subpattern-frame (:pattern parent) [eid] false ::recursion)
         :recursion (:recursion parent)))

(defn- pull-recursion-res-frame
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

(defn- pull-wildcard-frame
  [db [frame & frames]]
  (let [{:keys [eid spec pattern]} frame
        except (.-except spec)
        datoms (cond->> (group-by #(.-a %) (dc/-datoms db :eavt [eid]))
                 (seq except) (into [] (remove (comp except first))))]
    (conj frames {:state :expand :kvps (transient {:db/id eid})
                  :eid eid :pattern pattern :datoms (seq datoms)})))

(defn- pull-pattern
  [db frames]
  (case (:state (first frames))
    :attr          (recur db (pull-attr-frame db frames))
    :attr-reverse  (recur db (pull-attr-reverse-frame db frames))
    :default       (recur db (pull-default-frame db frames))
    :expand        (recur db (pull-expand-frame db frames))
    :limit         (recur db (pull-limit-frame db frames))
    :map-entry     (recur db (pull-map-entry-key-frame db frames))
    :map-entry-val (recur db (pull-map-entry-val-frame db frames))
    :map-entry-res (recur db (pull-map-entry-res-frame db frames))
    :recursion     (recur db (pull-recursion-frame db frames))
    :recursion-res (recur db (pull-recursion-res-frame db frames))
    :pattern       (recur db (pull-pattern-frame db frames))
    :wildcard      (recur db (pull-wildcard-frame db frames))
    :done          (let [[f & remaining] frames
                         result (cond-> (persistent! (:results f))
                                  (not (:multi? f)) first)]
                     (if (seq remaining)
                       (->> (cond-> (first remaining)
                              result (update :kvps assoc! (:attr f) result))
                            (conj (rest remaining))
                            (recur db))
                       result))))

(defn- pull-selector
  [db selector eids multi?]
  (let [eids    (into [] (comp (remove not) (map #(dc/entid-strict db %))) eids)
        pattern (pp/parse-pull selector)]
    (pull-pattern db (list (initial-frame pattern eids multi?)))))

(defn pull
  [db selector eid]
  (pull-selector db selector [eid] false))

(defn pull-many
  [db selector eids]
  (pull-selector db selector eids true))
