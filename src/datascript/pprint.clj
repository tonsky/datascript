(ns datascript.pprint
  (:require [datascript.db :as db]
            [clojure.pprint :as pp])
  (:import [datascript.db Datom DB FilteredDB]))

(defmethod pp/simple-dispatch Datom [^Datom d]
  (pp/pprint-logical-block :prefix "#datascript/Datom [" :suffix "]"
                           (pp/write-out (.-e d))
                           (.write ^java.io.Writer *out* " ")
                           (pp/pprint-newline :linear)
                           (pp/write-out (.-a d))
                           (.write ^java.io.Writer *out* " ")
                           (pp/pprint-newline :linear)
                           (pp/write-out (.-v d))
                           (.write ^java.io.Writer *out* " ")
                           (pp/pprint-newline :linear)
                           (pp/write-out (db/datom-tx d))
                           (.write ^java.io.Writer *out* " ")
                           (pp/pprint-newline :linear)
                           (pp/write-out (db/datom-added d))))

(defn- pp-db [db ^java.io.Writer w]
  (pp/pprint-logical-block :prefix "#datascript/DB {" :suffix "}"
                           (pp/pprint-logical-block
                            (pp/write-out :schema)
                            (.write w " ")
                            (pp/pprint-newline :linear)
                            (pp/write-out (db/-schema db)))
                           (.write w ", ")
                           (pp/pprint-newline :linear)

                           (pp/pprint-logical-block
                            (pp/write-out :datoms)
                            (.write w " ")
                            (pp/pprint-newline :linear)
                            (pp/pprint-logical-block :prefix "[" :suffix "]"
                                                     (pp/print-length-loop [aseq (seq db)]
                                                                           (when aseq
                                                                             (let [^Datom d (first aseq)]
                                                                               (pp/write-out [(.-e d) (.-a d) (.-v d) (db/datom-tx d)])
                                                                               (when (next aseq)
                                                                                 (.write w " ")
                                                                                 (pp/pprint-newline :linear)
                                                                                 (recur (next aseq))))))))))

(defmethod pp/simple-dispatch DB [db] (pp-db db *out*))
(defmethod pp/simple-dispatch FilteredDB [db] (pp-db db *out*))
