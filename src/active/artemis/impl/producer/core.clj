(ns active.artemis.impl.producer.core
  "Implementation of the [[active.artemis.protocol.producer/producer]] protocol,
  connecting to an Apache Artemis instance using
  a [[active.artemis.impl.connection-strategy/connection-strategy]] for
  connection params and
  [[active.artemis.impl.connection-strategy/authentication-credentials]] for
  authentication if needed.

  Create an instance of a producer that satisfies the protol via [[make]]"
  (:require
   [active.artemis.impl.connection-strategy :as connection-strategy]
   [active.artemis.protocol.producer :as producer])
  (:import
   [org.apache.activemq.artemis.api.core.client ClientMessage ClientProducer ClientSession]))

(defn- create-client-producer
  ^ClientProducer [^ClientSession client-session address]
  (.createProducer client-session address))

(defn- make-send-message!
  "Takes a started `client-sessions` and a `client-producer` and returns a
  function that sends messages using that producer to Artemis."
  [^ClientSession client-session ^ClientProducer client-producer]
  (fn send-message! [message-content]
    (let [^ClientMessage message (.createMessage client-session true)]
      (.writeString (.getBodyBuffer message) message-content)
      (.send client-producer message))))

(defn- make-start!
  [^ClientSession client-session]
  (fn start! []
    (.start client-session)))

(defn- make-stop!
  [client-session-state]
  (fn stop! [producer-ref]
    (connection-strategy/close-client-session-state! client-session-state)
    (.close producer-ref)))

;; TODO (Marco): Add clientId to identify our application for debugging/management.
;; SEE: https://artemis.apache.org/components/artemis/documentation/latest/core.html#identifying-your-client-application-for-management-and-debugging
(defn make
  "Takes a [[active.artemis.impl.connection-strategy/connection-strategy]] and
  a [[active.artemis.protocol.producer/producer-configuration]] and returns a
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

