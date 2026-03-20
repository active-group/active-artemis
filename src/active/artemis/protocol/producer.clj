(ns active.artemis.protocol.producer
  "# Producers for Artemis Messages

  This namespace defines the protocol for Apache Artemis producers. You can find
  implementations for that protocol under [[active.artemis.impl.producer]].

  ## Producer Configuration
  
  A [[producer-configuration]] defines a topic (an address in Artemis parlens)
  the producer publishes messages to. It is the mere description of the intent
  to publish messages to that address. To actually 'run' a producer, we need an
  implementation of a [[prodcuer]] (see below).

  Create a new [[producer-configurtion]] via [[make-producer-configuration]].

  ## A Producer Process
  
  The [[producer]] protocol (a record of functions) handles the lifecycle of a
  producer. You can define producers using the [[make]] functions. You can
  interact with the [[producer]] with these three functions:

  * [[send-message!]]: Publish a message through the consumer to the address
  defined by the [[producer-configuration]] used to define the producer. There
  is no useful return value for messages sent to the producer.
  * [[start!]]: Starts the producer and returns a reference to the producer
  object.
  * [[stop!]]: Stops the producer and cleans up everything.
  "
  (:require
   [active.data.realm :as realm]
   [active.data.record :as r]))

(def realm:address realm/string)

(r/def-record
  ^{:doc "A [[producer-configuration]] consists of an address that messages are sent to."}
  producer-configuration
  [producer-configuration-address :- realm:address])

(defn make-producer-configuration
  "Make an new [[producer-configuration]] that intents to publish messages to a
  particular `address`."
  [address]
  (producer-configuration producer-configuration-address address))

(defn producer-configuration?
  "Is a `thing` a [[producer-configuration]]?"
  [thing]
  (r/is-a? producer-configuration thing))

(def realm:message realm/string)
(def realm:producer-ref realm/any)

(r/def-record producer
  [producer-send-message! :- (realm/function realm:message -> realm/any)
   producer-start! :- (realm/function -> realm/any)
   producer-stop! :- (realm/function realm:producer-ref -> realm/any)])

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
  "Start the `producer`. Returns a reference to the actual producer object, which
  must be passed to [[stop!] for cleanup of resources when you stop the
  producer."
  [producer]
  ((producer-start! producer)))

(defn stop!
  "Stop the `producer`, cleaning up after `producer-ref`. `producer-ref` is the
  object you get by calling [[start!]] for the `producer`."
  [producer producer-ref]
  ((producer-stop! producer) producer-ref))
