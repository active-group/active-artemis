(ns active.artemis.core
  (:require
   [active.artemis.impl.connection-strategy :as conn]
   [active.artemis.impl.consumer.core :as consumer-impl]
   [active.artemis.impl.producer.core :as producer-impl]
   [active.artemis.protocol.consumer :as consumer]
   [active.artemis.protocol.producer :as producer]
   [active.clojure.logger.event :as logger]))

(defn- echo-message-handler [message]
  (logger/log-event! :info (logger/log-msg "Got message:" message)))

(defn -main []
  (let [host+port (conn/make-remote-host-strategy "tcp://localhost:61616")
        credentials (conn/make-username+password-credentials "artemis"
                                                             "artemis")
        producer (producer-impl/make host+port
                                     credentials
                                     (producer/make-producer-configuration "active.news"))
        consumer (consumer-impl/make host+port
                                     credentials
                                     (consumer/make-consumer-configuration "active.news"
                                                                           echo-message-handler))]
    (producer/start! producer)
    (consumer/start! consumer)
    (producer/send-message! producer "Hi from AG-HQ")
    (producer/send-message! producer "A second message from me")
    (producer/send-message! producer "Going home now -- baba!")
    (producer/stop! producer)
    (consumer/stop! consumer)))
