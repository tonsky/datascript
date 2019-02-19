#!/usr/bin/env clojure

"USAGE: ./bench [rebuild]? [<version>|<version-vm> ...]? [<bench-name> ...]?"

(require
  '[clojure.edn :as edn]
  '[clojure.java.io :as io]
  '[clojure.java.shell :as sh]
  '[clojure.string :as str])

(defn sh [& cmd]
  (let [res (apply sh/sh cmd)]
    (when (not= 0 (:exit res))
      (throw (ex-info "ERROR" res)))
    (str/trim (:out res))))

(defn copy [^java.io.InputStream input ^java.io.Writer output]
  (let [^"[C" buffer (make-array Character/TYPE 1024)
        in (java.io.InputStreamReader. input "UTF-8")
        w  (java.io.StringWriter.)]
    (loop []
      (let [size (.read in buffer 0 (alength buffer))]
        (if (pos? size)
          (do (.write output buffer 0 size)
              (.flush output)
              (.write w buffer 0 size)
              (recur))
          (str w))))))

(defn run [& cmd]
  (let [cmd  (remove nil? cmd)
        proc (.exec (Runtime/getRuntime)
                    (into-array String cmd)
                    (@#'sh/as-env-strings sh/*sh-env*)
                    (io/as-file sh/*sh-dir*))
        out  (promise)]
    (with-open [stdout (.getInputStream proc)
                stderr (.getErrorStream proc)]
      (future (deliver out (copy stdout *out*)))
      (future (copy stderr *err*))
      (.close (.getOutputStream proc))
      (let [code (.waitFor proc)]
        (when (not= code 0)
          (throw (ex-info "ERROR" {:cmd cmd :code code})))
        @out))))


(def opts
  (loop [opts {:rebuild    false
               :versions   []
               :benchmarks []}
         args *command-line-args*]
    (if-some [arg (first args)]
      (cond
        (= "rebuild" arg)
        (recur (assoc opts :rebuild true) (next args))

        (re-matches #"(jvm|v8|datomic)" arg)
        (recur (update opts :versions conj ["latest" arg]) (next args))

        (re-matches #"(\d+\.\d+\.\d+|[0-9a-fA-F]{40}|latest)" arg)
        (recur (update opts :versions conj [arg "jvm"]) (next args))

        (re-matches #"(\d+\.\d+\.\d+|[0-9a-fA-F]{40}|latest)-(jvm|v8|datomic)" arg)
        (let [[_ version vm] (re-matches #"(\d+\.\d+\.\d+|[0-9a-fA-F]{40}|latest)-(jvm|v8|datomic)" arg)]
          (recur (update opts :versions conj [version vm]) (next args)))

        :else
        (recur (update opts :benchmarks conj arg) (next args)))
      opts)))


(defn run-benchmarks [version vm benchmarks]
  (case vm
    "jvm"
    (apply run "clojure" "-Sdeps"
      (cond
        (= "latest" version)
        (str "{:paths [\"src\"]"
          "    :deps {datascript {:local/root \"..\"}}}")

        (re-matches #"\d+\.\d+\.\d+" version)
        (str "{:paths [\"src\"]"
          "    :deps {datascript {:mvn/version \"" version "\"}}}")

        (re-matches #"[0-9a-fA-F]{40}" version)
        (str "{:paths [\"src\"]"
          "    :deps {datascript {:git/url \"https://github.com/tonsky/datascript.git\" :sha \"" version "\"}}}"))
      "-m" "datascript-bench.datascript"
      benchmarks)

    "v8"
    (apply run "node" "run_v8.js" benchmarks)

    "datomic"
    (apply run "clojure" "-Sdeps"
      (str "{"
        " :paths [\"src\" \"src-datomic\"]"
        " :deps {com.datomic/datomic-free {:mvn/version \"" (if (= "latest" version) "0.9.5703" version) "\"}}"
        "}")
      "-m" "datascript-bench.datomic"
      benchmarks)
    ))

 
(def default-benchmarks
  ["add-1"
   "add-5"
   "add-all"
   "init"
   "retract-5"
   "q1"
   "q2"
   "q3"
   "q4"
   "qpred1"
   "qpred2"])


(def default-versions
  [["latest" "jvm"]])


(binding [sh/*sh-env* (merge {} (System/getenv) {})
          sh/*sh-dir* "."]
  (let [{:keys [rebuild benchmarks versions]} opts]
    (when rebuild
      (binding [sh/*sh-dir* ".."]
        (run "lein" "do" "clean," "javac," "cljsbuild" "once" "bench")))
    (let [benchmarks (if (empty? benchmarks) default-benchmarks benchmarks)
          versions   (if (empty? versions)   default-versions    versions)]
      (print "version   \t")
      (doseq [b benchmarks] (print b "\t"))
      (println)
      (doseq [[version vm] versions]
        (print (str version "-" vm) "\t")
        (flush)
        (run-benchmarks version vm benchmarks)))))

(shutdown-agents)
; (System/exit 0)
