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
            [langohr.basic     :as lb]))


(def ^{:const true}
  default-exchange-name "")

(def mq-map (atom {}))
(def conn-map (ref {:uuid {} :conn {}}))
(def req-queue-name "face.detection.request" )
(def resp-queue-name "face.detection.response")

 (defn init-mq [ req-qname resp-qname ]
	(let [ cn (rmq/connect)
	       c ( lch/open cn )]
		(lq/declare c req-qname :exclusive false :auto-delete false)
    (lq/declare c resp-qname :exclusive false :auto-delete false)
		{ :conn cn :ch c }))



(defn get-conn [ uuid ]
  (get (:uuid @conn-map) uuid ))

(defn get-uuid [ conn ]
  (get (:conn @conn-map) conn ))


(defn add-conn-uuid [ c uuid]
  (println (str "addi ng connection " c " with uuid " uuid))
  (dosync
    (ref-set conn-map (assoc-in @conn-map [:conn c ] uuid))
    (ref-set conn-map (assoc-in @conn-map [:uuid uuid] c ))
  ))

(defn remove-conn-uuid [ c ]
  (let [ uuid (get-uuid c) ]
     (println (str "removing connection " c " with uuid " uuid))
    (dosync
      (ref-set conn-map (core/dissoc-in @conn-map [:conn c ]  ))
      (ref-set conn-map (core/dissoc-in @conn-map [:uuid uuid] ))
  )))



(defn send-message [ connection message ]
	(println (str "conn:" connection " message:" message ))
    (.send connection (json/json-str
                       {:type "upcased" :message (s/upper-case message) })))
(defn message-handler
  [ch {:keys [content-type delivery-tag type] :as meta} ^bytes payload]
  (comment (println (format "[consumer] Received a message: %s, delivery tag: %d, content type: %s, type: %s"
                   (String. payload "UTF-8") delivery-tag content-type type)))
	(let [ 	s (String. payload "UTF-8") 
		m (json/read-json s) 
		uuid (:uuid m )] 
		(println (str "uuid in message:" uuid))
		(send-message (get-conn uuid ) (str (:message m) " response"))))

(defn start-consumer
  "Starts a consumer in a separate thread"
  [conn ch queue-name]
  (let [thread (Thread. (fn []
                          (lc/subscribe ch queue-name message-handler :auto-ack true)))]
    (.start thread)))

(defn produce-message [ ch qname m ]
	(lb/publish ch default-exchange-name qname m :content-type "application/javascript" :type "greetings.hi"))

(defn on-message [connection json-message]
  (let [message (-> json-message json/read-json (get-in [:data :message]))
	uuid (get-uuid connection )]
	(println (str "message:" message " with uuid:" uuid ))
	(produce-message (:ch @mq-map) req-queue-name (json/json-str {:message message :uuid uuid}))))

(defn on-close [ c ]
		(remove-conn-uuid c))

(defn on-open [ c ]

    (add-conn-uuid c (str (java.util.UUID/randomUUID))))

(defn -main []

	(let [ m (init-mq req-queue-name resp-queue-name )]
		(swap! mq-map merge m)
		(println "amqp client initialized"))
		(println @mq-map)
	(start-consumer (:conn @mq-map) (:ch @mq-map ) resp-queue-name )

  (doto (WebServers/createWebServer 8080)
    (.add "/websocket"
          (proxy [WebSocketHandler] []
            (onOpen [c] (on-open c))
            (onClose [c] (on-close c))
            (onMessage [c j] (on-message c j))))

    (.add (StaticFileHandler. "."))
    (.start)))

