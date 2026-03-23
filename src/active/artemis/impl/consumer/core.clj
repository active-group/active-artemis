(ns active.artemis.impl.consumer.core
  "Implementation of the [[active.artemis.protocol.consumer/consumer]] protocol,
  connecting to an Apache Artemis instance using
  a [[active.artemis.impl.connection-strategy/connection-strategy]] for
  connection params and
  [[active.artemis.impl.connection-strategy/authentication-credentials]] for
  authentication if needed.

  Create an instance of a consumer that satisfies the protol via [[make]]."
  (:require
   [active.artemis.impl.connection-strategy :as connection-strategy]
   [active.artemis.protocol.consumer :as consumer])
  (:import
   [org.apache.activemq.artemis.api.core RoutingType]
   [org.apache.activemq.artemis.api.core QueueConfiguration]
   [org.apache.activemq.artemis.api.core.client MessageHandler]
   [org.apache.activemq.artemis.api.core.client ClientConsumer ClientSession]
   [java.util.concurrent CountDownLatch]))

(defn- gen-queue-name [address]
  (str `active.artemis.impl.consumer.core "-" address "-" (gensym)))

;; NOTE: We cannot receive messages 'directly' but have to define a queue where
;; artemis can enqueue messages for us. We then take from the queue. The queue
;; is configured to 'die' when the associated ClientSession is closed.
(defn- create-queue!
  "A queue must be created before starting our handler."
  [^ClientSession client-session address queue-name]
  (let [^QueueConfiguration queue-configuration (doto (QueueConfiguration. queue-name)
                                                  (.setAddress address)
                                                  ;; TODO: Konfigurierbar machen
                                                  (.setRoutingType RoutingType/MULTICAST)
                                                  (.setDurable false)
                                                  (.setAutoDelete true)
                                                  (.setAutoDeleteDelay 0)
                                                  (.setAutoDeleteMessageCount 0))]
    ;; NOTE: Unfortunately, there is not handle on a queue (`(.createQueue
    ;; clientSession)` returns `void`).
    (try (.createQueue client-session
                       queue-configuration)
         (catch Exception _e
           nil))))

(defn- is-poison-pill? [message]
  (and (.containsProperty message "isLastMessage")
       (.getBooleanProperty message "isLastMessage")))

(defn- make-start! [^ClientSession client-session address queue-name create-queue? message-handler]
  (fn start! [wait-for-poison-pill?]
    (.start client-session)
    (when create-queue? (create-queue! client-session address queue-name))
    (let [^ClientConsumer consumer (.createConsumer client-session queue-name)
          ^CountDownLatch maybe-completion-latch (when wait-for-poison-pill?
                                                   (CountDownLatch. 1))
          handler (reify MessageHandler
                    (onMessage [_this msg]
                      (let [body (.. msg getBodyBuffer readString)]
                        (message-handler body)
                        (when (and wait-for-poison-pill?
                                   (is-poison-pill? msg))
                          (.countDown maybe-completion-latch))
                        (.acknowledge msg))))]
      (.setMessageHandler consumer handler)
      (if wait-for-poison-pill?
        [consumer maybe-completion-latch]
        consumer))))

(defn- make-stop! [client-session-state]
  (fn [consumer-ref]
    (connection-strategy/close-client-session-state! client-session-state)
    (.close consumer-ref)))

(defn make
  "Takes a [[active.artemis.impl.connection-strategy/connection-strategy]] and
  a [[active.artemis.protocol.consumer/consumer-configuration]] and returns a
  new [[active.artemis.protocol.consumer/consumer-impl]].

  `authentication-credentials` defines if and how authentication is performed
  and must be a valid [[authentication-credentials]] map."
  [connection-strategy authentication-credentials consumer-configuration]
  (let [address (consumer/consumer-configuration-address consumer-configuration)
        external-queue? (consumer/consumer-configuration-with-external-queue? consumer-configuration)
        queue-name (if external-queue?
                     (consumer/consumer-configuration-external-queue-name consumer-configuration)
                     (gen-queue-name address))
        client-session-state (connection-strategy/create-client-session connection-strategy
                                                                        authentication-credentials)
        ^ClientSession client-session (connection-strategy/get-client-session-state client-session-state)]
    (consumer/make (make-start! client-session
                                (consumer/consumer-configuration-address consumer-configuration)
                                queue-name
                                ;; NOTE: If there is no external queue
                                ;; configured, instruct the start function to
                                ;; create one when called.
                                (not external-queue?)
                                (consumer/consumer-configuration-message-handler consumer-configuration))
                   (make-stop! client-session-state))))
