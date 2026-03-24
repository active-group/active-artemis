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
   [active.data.record :as r])
#_  (:import
   [clojure.core.async.impl.channels ManyToManyChannel]))

(def realm:address realm/string)

(r/def-record consumer-configuration
  [consumer-configuration-address :- realm:address
   consumer-configuration-buffer-size :- realm/natural])

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
  [address & [{:keys [queue-name buffer-size]
               :or {queue-name nil
                    buffer-size 64}}]]
  (if (some? queue-name)
    (consumer-configuration-with-external-queue consumer-configuration-address address
                                                consumer-configuration-buffer-size buffer-size
                                                consumer-configuration-external-queue-name queue-name)
    (consumer-configuration consumer-configuration-address address
                            consumer-configuration-buffer-size buffer-size)))

(defn consumer-configuration?
  "Is a `thing` a [[consumer-configuration]]?"
  [thing]
  (r/is-a? consumer-configuration thing))

(defn consumer-configuration-with-external-queue?
  "Is a `thing` a [[consumer-configuration-with-external-queue]]?"
  [thing]
  (r/is-exactly-a? consumer-configuration-with-external-queue
                   thing))

(r/def-record message
  [message-body :- realm/string
   message-acknowledge! :- (realm/function -> realm/any)])

(defn make-message
  "Takes an artemis message and wraps it into a [[message]] object."
  [msg]
  (let [body (.. msg getBodyBuffer readString)]
    (message message-body body
             message-acknowledge! (fn []
                                    (.acknowledge msg)))))

(defn mark-as-read!
  "Mark a [[message]] as read."
  [message]
  ((message-acknowledge! message)))

(def realm:consumer-ref realm/any)
#_(def realm:chan (realm/from-predicate "Realm for [[clojure.core.async/chan]]s."
                                      #(instance? ManyToManyChannel %)))

(r/def-record consumer
  [consumer-receiver-chan :- realm/any ; realm:chan
   consumer-start! :- (realm/function -> realm/any)
   consumer-stop! :- (realm/function realm:consumer-ref -> realm/any)])

(defn make
  "Make a new [[consumer]]. See [[consumer]] for function signatures of the
  arguments"
  [receiver-chan start! stop!]
  (consumer consumer-receiver-chan receiver-chan
            consumer-start! start!
            consumer-stop! stop!))

(defn message-chan
  "Get a handle on the `consumer`s receiver
  chan (a [[clojure.core.async.impl.channels.ManyToManyChannel]])."
  [consumer]
  (consumer-receiver-chan consumer))

(defn start!
  "Start the `consumer`. Returns a reference to the actual consumer object, which
  must be passed to [[stop!]] for cleanup of resources when you stop the
  consumer."
  [consumer]
  ((consumer-start! consumer)))

(defn stop!
  "Stop the `consumer`, cleaning up after `consumer-ref`. `consumer-ref` is the
  object you get by calling [[start!]] for the `consumer`."
  [consumer consumer-ref]
  ((consumer-stop! consumer) consumer-ref))

