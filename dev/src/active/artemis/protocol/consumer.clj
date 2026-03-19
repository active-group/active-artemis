(ns active.artemis.protocol.consumer
  (:require
   [active.data.realm :as realm]
   [active.data.record :as r]))

(def realm:address realm/string)

(r/def-record consumer-configuration
  [consumer-configuration-address :- realm:address
   consumer-configuration-message-handler :- (realm/function realm/any -> realm/any)])

(defn make-consumer-configuration
  [address callback]
  (consumer-configuration consumer-configuration-address address
                          consumer-configuration-message-handler callback))

(defn consumer-configuration?
  "Is a `thing` a [[consumer-configuration]]?"
  [thing]
  (r/is-a? consumer-configuration thing))

(def realm:message realm/string)

(r/def-record consumer
  [consumer-start! :- (realm/function -> realm/any)
   consumer-stop! :- (realm/function -> realm/any)])

(defn make
  "Make a new [[consumer]]. See [[consumer]] for function signatures of the
  arguments"
  [start! stop!]
  (consumer consumer-start! start!
            consumer-stop! stop!))

(defn start!
  "Start the `consumer`."
  [consumer]
  ((consumer-start! consumer)))

(defn stop!
  "Stop the `consumer`."
  [consumer]
  ((consumer-stop! consumer)))
