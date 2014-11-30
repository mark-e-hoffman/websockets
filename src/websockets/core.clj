(ns websockets.core
  (:require [clojure.data.json :as json]
            [clojure.string :as s])
  (:require [clojure.core.incubator :as core])
  (:import [org.webbitserver WebServer WebServers WebSocketHandler]
           [org.webbitserver.handler StaticFileHandler])
  (:require [langohr.core      :as rmq]
            [langohr.channel   :as lch]
            [langohr.queue     :as lq]
            [langohr.consumers :as lc]
	    [langohr.exchange  :as le]
            [langohr.basic     :as lb])
 (:gen-class))


(def ^{:const true}
  default-exchange-name "")

(def mq-map (atom {}))
(def conn-map (ref {}))
(def queue-map (ref {}))

(defn init-mq [exchange ]
	(let [ cn (rmq/connect)
		c ( lch/open cn )]
		(le/declare c exchange "topic" {:durable false :auto-delete true})
		{ :conn cn :ch c }))

(defn get-channel []
	(:ch @mq-map))

(defn get-conn [ qname ]
  (get (:conn @queue-map) qname ))

(defn get-qname [ conn ]
  (:qname (get @conn-map) conn ))

(defn add-conn [ c ]
  (dosync
    (ref-set conn-map (assoc @conn-map c {}))
  ))

(defn add-conn-queue [ c qname ]
  (dosync
    (ref-set conn-map (assoc @conn-map c { :qname qname}))
    (ref-set queue-map (assoc @queue-map qname  {:conn c }))))

(defn remove-conn [ c ]
  (let [ qname  (get-qname c) ]
     (println (str "removing connection " c " with queue " qname ))
    (dosync
      (ref-set conn-map dissoc @conn-map c )
      (ref-set queue-map dissoc @queue-map qname ))))

(defn send-message [ connection ^String message routing-key]
    (.send connection (json/write-str {:type "event" :message message :routing_key routing-key})))

(defn start-consumer
  "Starts a consumer in a separate thread"
  [conn ch queue-name handler]
  (let [thread (Thread. (fn []
                          (lc/subscribe ch queue-name handler {:consumer-tag queue-name :auto-ack true})))]
    (.start thread)))

(defn on-message [connection json-message ]
 (println json-message)
  (let [jsdata  (json/read-str json-message :key-fn keyword)
	exchange (get-in jsdata [:data :exchange] )
	routing-key (get-in jsdata [:data :routing_key] )
	ch ( get-channel ) ]
 		(let [ 	qname (lq/declare-server-named ch {:exclusive true}) 
			my-handler (fn [ch meta ^bytes payload]   
					(let [ s (String. payload "UTF-8") ]
                				(send-message connection s (:routing-key meta)))) ]
		(lq/bind ch qname exchange {:routing-key routing-key}) 
		(add-conn-queue connection qname)
		(start-consumer (:conn @mq-map) ch qname my-handler)))
)

(defn on-close [ c ]
	(remove-conn c))

(defn on-open [ c ]
    (add-conn c ))

(defn -main []

	(let [ m (init-mq "my.exchange" )]
		(swap! mq-map merge m)
		(println "amqp client initialized"))
		(println @mq-map)

  (doto (WebServers/createWebServer 8080)
    (.add "/websocket"
          (proxy [WebSocketHandler] []
            (onOpen [c] (on-open c))
            (onClose [c] (on-close c))
            (onMessage [c j] (on-message c j))))

    (.add (StaticFileHandler. "."))
    (.start)))

