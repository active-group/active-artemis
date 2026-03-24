(ns active.artemis.core
  (:require
   [clojure.core.async :as async]
   [active.artemis.impl.connection-strategy :as conn]
   [active.artemis.impl.consumer.core :as consumer-impl]
   [active.artemis.impl.producer.core :as producer-impl]
   [active.artemis.protocol.consumer :as consumer]
   [active.artemis.protocol.producer :as producer]
   [active.clojure.logger.event :as logger]))

(defn looper
  [ch timeout-ms callback]
  (async/go-loop []
    (let [timeout-ch (async/timeout timeout-ms)
          [val port] (async/alts! [ch timeout-ch])]
      (when-not (or (= port timeout-ch)
                    (nil? val))
        (callback val)
        (recur)))))

(defn make-message-handler [label]
  (fn [msg]
    (logger/log-event! :info (format "%s received message %s"
                                     label
                                     (consumer/message-body msg)))
    (consumer/mark-as-read! msg)))

(defn -main []
  (let [host+port-live (conn/make-remote-host+port-strategy "localhost"
                                                            "61616")
        host+port-backup (conn/make-remote-host+port-strategy "localhost"
                                                         "61617")
        multi (conn/make-multi-remote-host+port-strategy
               host+port-backup
               host+port-live)
        
        credentials (conn/make-username+password-credentials "artemis"
                                                             "artemis")
        producer (producer-impl/make multi
                                     credentials
                                     (producer/make-producer-configuration "active.news"))
        consumer1 (consumer-impl/make host+port-live
                                      credentials
                                      (consumer/make-consumer-configuration "active.news"))
        consumer2 (consumer-impl/make host+port-backup
                                      credentials
                                      (consumer/make-consumer-configuration "active.news"))

        chan1 (looper (consumer/consumer-receiver-chan consumer1)
                      5000
                      (make-message-handler "Consumer 1"))

        chan2 (looper (consumer/consumer-receiver-chan consumer2)
                      5000
                      (make-message-handler "Consumer 2"))
        
        producer-ref (producer/start! producer)
        consumer-ref1 (consumer/start! consumer1)
        consumer-ref2 (consumer/start! consumer2)]
    (-> producer
        (producer/send-message! "Hi from AG-HQ")
        (producer/send-message! "A second message from me")
        (producer/send-message! "Going home now -- baba!"))
    (producer/stop! producer producer-ref)
    (consumer/stop! consumer1 consumer-ref1)
    (consumer/stop! consumer2 consumer-ref2)

    (async/close! chan1)
    (async/close! chan2)))

(-main)
