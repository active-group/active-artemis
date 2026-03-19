(ns active.artemis.protocol.producer
  (:require
   [active.data.realm :as realm]
   [active.data.record :as r]))

(def realm:address realm/string)

(r/def-record
  ^{:doc "A [[producer-configuration]] consists of an address that messages are sent to."}
  producer-configuration
  [producer-configuration-address :- realm:address])

(defn make-producer-configuration
  "Make an new [[producer-configuration]] with a particular `address`."
  [address]
  (producer-configuration producer-configuration-address address))

(defn producer-configuration?
  "Is a `thing` a [[producer-configuration]]?"
  [thing]
  (r/is-a? producer-configuration thing))

(def realm:message realm/string)

(r/def-record producer
  [producer-send-message! :- (realm/function realm:message -> realm/any)
   producer-start! :- (realm/function -> realm/any)
   producer-stop! :- (realm/function -> realm/any)])

(defn make
  "Make a new [[producer]]. See [[producer]] for function signatures of the
  arguments."
  [start! send-message! stop!]
  (producer producer-send-message! send-message!
            producer-start! start!
            producer-stop! stop!))

(defn send-message!
  "Send a `message` for publication to the `producer`."
  [producer message]
  ((producer-send-message! producer) message))

(defn start!
  "Start the `producer`."
  [producer]
  ((producer-start! producer)))

(defn stop!
  "Stop the `producer`."
  [producer]
  ((producer-stop! producer)))
