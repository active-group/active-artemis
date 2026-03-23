(ns active.artemis.protocol.consumer
  "# Consumers for Artemis Messages

  This namespace defines the protocol for Apache Artemis consumers. You can find
  implementations for that protocol under [[active.artemis.impl.consumer]].

  ## Consumer Configuration 

  A [[consumer-configuration]] defines a topic (an address in Artemis parlens)
  the consumer is subscribed to and a message handler that processes messages
  that arrive for that topic. It is the mere description of the intent to listen
  to that topic. To actually 'run' a consumer, we need an implementation of
  a [[consumer]] (see below).

  Create a new [[consumer-configurtion]] via [[make-consumer-configuration]].

  ## A Consumer Process

  The [[consumer]] protocol (a record of functions) handles the lifecycle of a
  consumer. You can define consumers using the [[make]] functions. You can
  interact with the [[consumer]] with these two functions:

  * [[start!]]: Starts the consumer and returns a reference to the consumer
  object.
  * [[stop!]]: Stops the consumer and cleans up everything."
  (:require
   [active.data.realm :as realm]
   [active.data.record :as r]))

(def realm:address realm/string)
(def realm:routing-type (realm/enum ::anycast ::multicast))

(r/def-record consumer-configuration
  [consumer-configuration-address :- realm:address
   consumer-configuration-message-handler :- (realm/function realm/any -> realm/any)])

(r/def-record consumer-configuration-with-external-queue
  :extends consumer-configuration
  [consumer-configuration-external-queue-name :- realm/string])

(defn make-consumer-configuration
  "Creates a new [[consumer-configuration]]. `address` must be a valid Artemis
  address (a string). `callback` is a function that takes one argument (the
  message that we receive from that address). Its return value is ignored.

  By default, a consumer creates and manages its own queue if started. If
  `:queue-name` (string) is provided in the opts-map, the consumer will not
  create a queue but instead will use the queue identified by `queue-name`. It
  waits for messages on a queue identified by the queue name. The caller must
  ensure the queue exists and get messages from artemis and clean it up
  afterwards."
  [address callback & [{:keys [queue-name]
                        :or {queue-name nil}}]]
  (if (some? queue-name)
    (consumer-configuration-with-external-queue consumer-configuration-address address
                                                consumer-configuration-message-handler callback
                                                consumer-configuration-external-queue-name queue-name)
    (consumer-configuration consumer-configuration-address address
                            consumer-configuration-message-handler callback)))

(defn consumer-configuration?
  "Is a `thing` a [[consumer-configuration]]?"
  [thing]
  (r/is-a? consumer-configuration thing))

(defn consumer-configuration-with-external-queue?
  "Is a `thing` a [[consumer-configuration-with-external-queue]]?"
  [thing]
  (r/is-exactly-a? consumer-configuration-with-external-queue
                   thing))

(def realm:consumer-ref realm/any)

(r/def-record consumer
  [consumer-start! :- (realm/function -> realm/any)
   consumer-stop! :- (realm/function realm:consumer-ref -> realm/any)])

(defn make
  "Make a new [[consumer]]. See [[consumer]] for function signatures of the
  arguments"
  [start! stop!]
  (consumer consumer-start! start!
            consumer-stop! stop!))

(defn start!
  "Start the `consumer`. Returns a reference to the actual consumer object, which
  must be passed to [[stop!]] for cleanup of resources when you stop the
  consumer."
  [consumer]
  ((consumer-start! consumer) false))

(defn start-with-completion-latch!
  "Start the `consumer`. Returns a tuple with a reference to the actual consumer
  object, which must be passed to [[stop!]] for cleanup of resources when you
  stop the consumer and a completion
  latch (a [[java.util.concurrent.CountDownLatch]]) that allows the main thread
  to wait for a completion message on that consumer before stopping it."
  [consumer]
  ((consumer-start! consumer) true))

(defn stop!
  "Stop the `consumer`, cleaning up after `consumer-ref`. `consumer-ref` is the
  object you get by calling [[start!]] for the `consumer`."
  [consumer consumer-ref]
  ((consumer-stop! consumer) consumer-ref))
