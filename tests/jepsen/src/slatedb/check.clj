(ns slatedb.check
  (:require [clojure.edn :as edn]
            [clj-time.core :as time]
            [clojure.java.io :as io]
            [clojure.pprint :as pprint]
            [elle.list-append :as append]
            [jepsen.checker :as checker]
            [jepsen.history :as history]
            [knossos.model :as model]))

(defn encode-absence [op]
  ;; Knossos treats nil reads as unknown. An absent key must have a concrete value.
  (if (and (nil? (:value op))
           (or (= :write (:f op)) (and (= :read (:f op)) (= :ok (:type op)))))
    (assoc op :value ::absent)
    op))

(defn check-history [workload events]
  (if-not (and (some #(= :invoke (:type %)) events)
               (some #(= :ok (:type %)) events))
    {:valid? false :error :no-successful-operations}
    (let [h (history/history events)]
      (case workload
        "register" (checker/check (checker/linearizable {:model (model/cas-register ::absent)})
                                   {:name "slatedb-etcd" :start-time (time/now)}
                                   (history/history (mapv encode-absence events)) {})
        "append" (append/check {:consistency-models [:strict-serializable]} h)
        (throw (ex-info "Workload must be register or append" {:workload workload}))))))

(defn -main [workload & paths]
  (when (empty? paths)
    (throw (ex-info "Usage: register|append HISTORY.edn ..." {})))
  (let [results (mapv (fn [path]
                       (let [result (assoc (check-history workload (edn/read-string (slurp path)))
                                           :path path :workload workload)
                             output (io/file (.getParentFile (io/file path))
                                             (str "jepsen-" workload "-result.edn"))]
                         (spit output (with-out-str (pprint/pprint result)))
                         result))
                     paths)]
    (pprint/pprint (mapv #(if (true? (:valid? %))
                          (select-keys % [:valid? :workload :path])
                          %)
                       results))
    (shutdown-agents)
    (System/exit (if (every? #(true? (:valid? %)) results) 0 1))))
