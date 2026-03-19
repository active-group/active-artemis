(ns active.artemis.impl.producer.core
  "In Artemis parlens, an 'address' is what you might think of as a 'topic'."
  (:require
   [active.artemis.impl.connection-strategy :as connection-strategy]
   [active.artemis.protocol.producer :as producer]
   [active.clojure.logger.event :as logger])
  (:import
   [org.apache.activemq.artemis.api.core RoutingType]
   [org.apache.activemq.artemis.api.core.client ClientMessage ClientProducer ClientSession]))

(defn create-client-producer
  ^ClientProducer [^ClientSession client-session address]
  (.createProducer client-session address))

(defn make-send-message!
  "Takes a started `client-sessions` and a `client-producer` and returns a
  function that sends messages using that producer to Artemis."
  [^ClientSession client-session ^ClientProducer client-producer]
  (fn send-message! [message-content]
    (let [^ClientMessage message (.createMessage client-session true)]
      (.writeString (.getBodyBuffer message) message-content)
      (.send client-producer message))))

(defn make-start!
  [^ClientSession client-session]
  (fn start! []
    (.start client-session)))

(defn make-stop!
  ;; FIXME (Marco): Also close the producer itself.
  [client-session-state]
  (fn stop! []
    (connection-strategy/close-client-session-state! client-session-state)))

;; TODO (Marco): Add clientId to identify our application for debugging/management.
;; SEE: https://artemis.apache.org/components/artemis/documentation/latest/core.html#identifying-your-client-application-for-management-and-debugging
(defn make
  "Takes a [[active.artemis.impl.connection-strategy/connection-strategy]] and
  a [[active.artemis.protocol.producer/producer]] and returns a
  new [[active.artemis.protocol.producer/producer-impl]].

  `authentication-credentials` defines if and how authentication is performed
  and must be a valid [[authentication-credentials]] map."
  [connection-strategy authentication-credentials producer-configuration]
  (let [client-session-state (connection-strategy/create-client-session connection-strategy
                                                                         authentication-credentials)
        ^ClientSession client-session (connection-strategy/get-client-session-state client-session-state)
        ^ClientProducer client-producer (create-client-producer client-session
                                                                (producer/producer-configuration-address producer-configuration))]
    (producer/make (make-start! client-session)
                   (make-send-message! client-session client-producer)
                   (make-stop! client-session-state))))

