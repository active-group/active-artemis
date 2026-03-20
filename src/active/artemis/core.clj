(ns active.artemis.core
  (:require
   [active.artemis.impl.connection-strategy :as conn]
   [active.artemis.impl.consumer.core :as consumer-impl]
   [active.artemis.impl.producer.core :as producer-impl]
   [active.artemis.protocol.consumer :as consumer]
   [active.artemis.protocol.producer :as producer]
   [active.clojure.logger.event :as logger]
   [clojure.string :as string]))

(defn- echo-message-handler [label message]
  (logger/log-event! :info (logger/log-msg label "got message:" message)))

(defn- screaming-message-handler [label message]
  (logger/log-event! :info (logger/log-msg label "got message:" (string/upper-case message))))

(defn -main []
  (let [host+port (conn/make-remote-host-strategy "tcp://localhost:61616")
        credentials (conn/make-username+password-credentials "artemis"
                                                             "artemis")
        producer (producer-impl/make host+port
                                     credentials
                                     (producer/make-producer-configuration "active.news"))
        consumer1 (consumer-impl/make host+port
                                      credentials
                                      (consumer/make-consumer-configuration "active.news"
                                                                            (partial echo-message-handler "CONSUMER 1")))
        consumer2 (consumer-impl/make host+port
                                      credentials
                                      (consumer/make-consumer-configuration "active.news"
                                                                            (partial screaming-message-handler "CONSUMER 2")))

        producer-ref (producer/start! producer)
        consumer-ref1 (consumer/start! consumer1)
        consumer-ref2 (consumer/start! consumer2)]
    (producer/send-message! producer "Hi from AG-HQ")
    (producer/send-message! producer "A second message from me")
    (producer/send-message! producer "Going home now -- baba!")
    (producer/stop! producer producer-ref)
    (consumer/stop! consumer1 consumer-ref1)
    (consumer/stop! consumer2 consumer-ref2)))

(-main)
