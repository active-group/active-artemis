(ns active.artemis.impl.consumer.core
  (:require
   [active.artemis.impl.connection-strategy :as connection-strategy]
   [active.artemis.protocol.consumer :as consumer])
  (:import
   [org.apache.activemq.artemis.api.core RoutingType]
   [org.apache.activemq.artemis.api.core QueueConfiguration]
   [org.apache.activemq.artemis.api.core.client MessageHandler]
   [org.apache.activemq.artemis.api.core.client ClientConsumer ClientSession]))

(defn- gen-queue-name [address]
  (str `active.artemis.impl.consumer.core "-" address "-" (gensym)))

(defn- create-queue!
  "A queue must be created before starting our handler."
  [^ClientSession client-session address queue-name]
  (let [^QueueConfiguration queue-configuration (doto (QueueConfiguration. queue-name)
                                                  (.setAddress address)
                                                  (.setRoutingType RoutingType/MULTICAST)
                                                  (.setDurable false)
                                                  (.setAutoDelete true)
                                                  (.setAutoDeleteDelay 0)
                                                  (.setAutoDeleteMessageCount 0))]
    (try (.createQueue client-session
                       queue-configuration)
         (catch Exception _e
           nil))))

(defn make-start! [^ClientSession client-session address queue-name message-handler]
  (fn start! []
    (.start client-session)
    (create-queue! client-session address queue-name)
    (let [^ClientConsumer consumer (.createConsumer client-session queue-name)
          handler (reify MessageHandler
                    (onMessage [_this msg]
                      (let [body (.. msg getBodyBuffer readString)]
                        (message-handler body)
                        (.acknowledge msg))))]
      (.setMessageHandler consumer handler)
      consumer)))

(defn make-stop! [client-session-state]
  (fn [consumer-ref]
    (connection-strategy/close-client-session-state! client-session-state)
    (.close consumer-ref)))

(defn make [connection-strategy authentication-credentials consumer-configuration]
  (let [address (consumer/consumer-configuration-address consumer-configuration)
        queue-name (gen-queue-name address)
        client-session-state (connection-strategy/create-client-session connection-strategy
                                                                        authentication-credentials)
        ^ClientSession client-session (connection-strategy/get-client-session-state client-session-state)]
    (consumer/make (make-start! client-session
                                (consumer/consumer-configuration-address consumer-configuration)
                                queue-name
                                (consumer/consumer-configuration-message-handler consumer-configuration))
                   (make-stop! client-session-state))))
