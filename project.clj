(defproject websockets "1.0.0-SNAPSHOT"
   :main websockets.core
  :description "FIXME: write description"
  :dependencies [
		[org.clojure/clojure "1.6.0"]
		[org.clojure/core.incubator "0.1.3"]
		[org.webbitserver/webbit "0.4.14"]
                [org.clojure/data.json "0.2.0"]
		[com.novemberain/langohr "3.0.0-rc2"]
		]
:aot [websockets.core]
)
