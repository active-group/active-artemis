(ns active.artemis.impl.core-test
  (:require
   [active.artemis.impl.connection-strategy :as conn]
   [active.artemis.impl.consumer.core :as consumer-impl]
   [active.artemis.impl.producer.core :as producer-impl]
   [active.artemis.protocol.consumer :as consumer]
   [active.artemis.protocol.producer :as producer]
   [clojure.core.async :as async]
   [clojure.test :as t])
  (:import
   [org.apache.activemq.artemis.core.config.impl ConfigurationImpl]
   [org.apache.activemq.artemis.core.server.embedded EmbeddedActiveMQ]))

(defn with-artemis!
  [server-id f]
  (let [uri (format "vm://%d" server-id)
        config (doto (ConfigurationImpl.)
                 (.setPersistenceEnabled false)
                 (.setSecurityEnabled false)
                 (.addAcceptorConfiguration "in-vm" uri))
        server (doto (EmbeddedActiveMQ.)
                 (.setConfiguration config))]
    (try
      (.start server)
      (let [connection-strategy (conn/make-in-vm-strategy server-id)]
        (f connection-strategy))
      (finally
        (.stop server)))))

(defn- drain-channel
  ;; Read `n` messages from the channel `ch`, waiting at most `timeout-ms`
  ;; each. Synchronously! Returns a vector of all read messages.
  [ch n timeout-ms]
  (loop [acc []]
    (if (= n (count acc))
      acc
      (let [[v _] (async/alts!! [ch (async/timeout timeout-ms)])]
        (if v
          (do (consumer/mark-as-read! v)
              (recur (conj acc (consumer/message-body v))))
          ;; timeout case
          acc)))))

(t/deftest core-producer-consumer-test
  (with-artemis! 0
    (fn [connection-strategy]
      (let [producer-cfg (producer/make-producer-configuration "test.address")
            consumer-cfg (consumer/make-consumer-configuration "test.address")
            producer (producer-impl/make connection-strategy
                                         (conn/make-no-credentials)
                                         producer-cfg)
            consumer (consumer-impl/make connection-strategy
                                         (conn/make-no-credentials)
                                         consumer-cfg)

            producer-ref (producer/start! producer)
            consumer-ref (consumer/start! consumer)
            message-ch (consumer/message-chan consumer)
            messages ["first message" "second message"]]
        ;; Setup message listener
        (doseq [message messages] (producer/send-message! producer message))

        (t/is (= messages
                 (drain-channel message-ch
                                (count messages)
                                500)))
        (producer/stop! producer producer-ref)
        (consumer/stop! consumer consumer-ref)))))

(t/deftest multiple-instances-test
  (with-artemis! 0
    (fn [connection-strategy-1]
      (with-artemis! 1
        (fn [connection-strategy-2]
          (t/testing "messages propagate through all connected instances"
            (let [combined-strategy (conn/make-multi-strategy connection-strategy-1
                                                              connection-strategy-2)
                  producer-cfg (producer/make-producer-configuration "test.address")
                  consumer-cfg (consumer/make-consumer-configuration "test.address")
                  producer (producer-impl/make combined-strategy
                                               (conn/make-no-credentials)
                                               producer-cfg)
                  consumer-1 (consumer-impl/make connection-strategy-1
                                                 (conn/make-no-credentials)
                                                 consumer-cfg)
                  consumer-2 (consumer-impl/make connection-strategy-2
                                                 (conn/make-no-credentials)
                                                 consumer-cfg)
                  producer-ref (producer/start! producer)
                  
                  consumer-ref-1 (consumer/start! consumer-1)
                  message-ch-1 (consumer/message-chan consumer-1)
                  
                  consumer-ref-2 (consumer/start! consumer-2)
                  message-ch-2 (consumer/message-chan consumer-2)
                  
                  messages ["message 1" "message 2"]]
              ;; Setup message listener
              (doseq [message messages] (producer/send-message! producer message))
              (t/testing "both consumers receive both messages on different connections"
                (t/is (= messages
                         (drain-channel message-ch-1
                                        (count messages)
                                        500)))
                (t/is (= messages
                         (drain-channel message-ch-2
                                        (count messages)
                                        500))))
              (producer/stop! producer producer-ref)
              (consumer/stop! consumer-1 consumer-ref-1)
              (consumer/stop! consumer-2 consumer-ref-2))))))))
