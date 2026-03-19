(require '[monkey.jms :as jms])

;; Connect.  The connection is auto-started.
(def ctx (jms/connect {:url "amqp://localhost:61616"
                       :username "artemis"
                       :password "artemis"}))

;; Start consuming.  In this case, it will just print the received message.
(def consumer (jms/consume ctx "topic://test.topic" println))

;; Producer is a fn that can be closed
(def producer (jms/make-producer ctx "topic://test.topic"))

;; Send a message
(producer "Hi, I'm a test message")

;; Stop consuming and producing
(.close producer)
(.close consumer)

;; Close connection
(.close ctx)
