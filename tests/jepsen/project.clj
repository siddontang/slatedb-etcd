(defproject slatedb-etcd-jepsen "0.1.0"
  :description "Check SlateDB etcd fault histories with Jepsen"
  :dependencies [[org.clojure/clojure "1.12.0"]
                 [jepsen "0.3.11"]]
  :repositories [["central" {:url "https://repo.maven.apache.org/maven2/"}]]
  :jvm-opts ["-Djava.awt.headless=true" "-Xmx2g"]
  :main slatedb.check)
