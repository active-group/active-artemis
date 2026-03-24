(ns active.artemis.impl.connection-strategy
  "# Producing Client Sessions

  This namespace defines different strategies to connect to Apache Artemis,
  a [[org.apache.activemq.artemis.api.core.client.ClientSession]].

  The key function is the [[create-client-session]], which takes
  a [[connection-strategy]] (see below) and
  some [[authentication-credentials]] (see below) and attempts to connect to an
  Artemis instance using the strategy and credentials. Upon a successfull
  connection, it returns a [[client-session-state]] that tracks the state of you
  session (see [[client-session-state]]).

  ## Connection Strategy

  A [[connection-strategy]] is one of the following:

  * A [[strategy-remote-host+port]] that describes the attempt to connect to a
  remote Artemis instance using a specific host and port. Create such a strategy
  via [[make-remote-host+port-strategy]].

  * A [[strategy-in-vm]], useful for connection to an Artemis instance that runs
  in the same process as your producer/consumer. Identified by an id. Create
  such a strategy via [[make-in-vm-strategy]].
  
  * A [[strategy-multi]] that consists of several (maybe nested)
  strategies. That strategy is useful if you have multiple Artemis instances you
  cant to listen/publish to at once.

  ## Authentication Credentials

  A map that satisfies the [[authentication-credentials]] record type is one of
  the following:

  * [[no-credentials]]: Used for unauthenticated connections to a Apache Artemis
  instance.
  * [[username+password]]: Used for authentication via username and password in
  authenticated connections to a Apache Artemis instance.

  ## Note on Authentication

  While the connection strategy is important for creating the client session
  factory, the actual authentication happens at the level of creating
  a [[ClientSession]] proper."
  (:require
   [active.data.realm :as realm]
   [active.data.record :as r])
  (:import
   [org.apache.activemq.artemis.api.core TransportConfiguration]
   [org.apache.activemq.artemis.api.core.client
    ActiveMQClient
    ClientSession
    ClientSessionFactory
    ServerLocator]
   [org.apache.activemq.artemis.core.remoting.impl.invm InVMConnectorFactory]
   [org.apache.activemq.artemis.core.remoting.impl.netty NettyConnectorFactory TransportConstants]))

(r/def-record connection-strategy [])

(r/def-record strategy-in-vm
  :extends connection-strategy
  [strategy-in-vm-server-id :- (realm/union realm/string
                                            realm/natural)])

(defn make-in-vm-strategy
  [server-id]
  (strategy-in-vm strategy-in-vm-server-id server-id))

(r/def-record strategy-remote-host+port
  :extends connection-strategy
  [strategy-remote-host+port-host :- realm/string
   strategy-remote-host+port-port :- realm/string])

(defn make-remote-host+port-strategy
  "Takes a `host` and `port` (both strings) and returns
  a [[strategy-remote-host+port]] for those values."
  [host port]
  (strategy-remote-host+port strategy-remote-host+port-host host
                             strategy-remote-host+port-port port))


(r/def-record strategy-multi
  :extends connection-strategy
  [strategy-multi-strategies :- (realm/sequence-of connection-strategy)])

(defn make-multi-strategy
  [& connection-strategies]
  (strategy-multi strategy-multi-strategies connection-strategies))

(defn connection-strategy->transport-configs
  [connection-strategy]
  (cond
    (r/is-a? strategy-in-vm connection-strategy)
    (let [params {"server-id" (str (strategy-in-vm-server-id connection-strategy))}
          ^TransportConfiguration transport-cfg (TransportConfiguration. (.getName InVMConnectorFactory)
                                                                         params)]
      [transport-cfg])
    
    (r/is-a? strategy-remote-host+port connection-strategy)
    (let [params {TransportConstants/HOST_PROP_NAME (strategy-remote-host+port-host connection-strategy)
                  TransportConstants/PORT_PROP_NAME (strategy-remote-host+port-port connection-strategy)}
          ^TransportConfiguration transport-cfg (TransportConfiguration. (.getName NettyConnectorFactory)
                                                                         params)]
      [transport-cfg])
    
    (r/is-a? strategy-multi connection-strategy)
    (mapcat connection-strategy->transport-configs
            (strategy-multi-strategies connection-strategy))))

;; Authentication credentials
(r/def-record authentication-credentials [])

(r/def-record username+password
  :extends authentication-credentials
  [username+password-username :- realm/string
   username+password-password :- realm/string])

(defn make-username+password-credentials
  "Make an new [[username+password]] authentication credentials map."
  [username password]
  (username+password username+password-username username
                     username+password-password password))

(r/def-record no-credentials :extends authentication-credentials [])

(defn make-no-credentials
  "Make an new [[no-credentials]] authentication credentials map."
  []
  (no-credentials))

(def realm:client-session (realm/from-predicate "Realm for objects of type [[ClientSession]]."
                                                #(instance? ClientSession %)))

(r/def-record client-session-state
  [client-session-state-get :- realm:client-session
   client-session-state-close! :- (realm/function -> realm/any)])

(defn get-client-session-state
  "Takes a [[client-session-state]] and returns the actual [[ClientSession]]."
  ^ClientSession [client-session-state]
  (client-session-state-get client-session-state))

(defn close-client-session-state!
  "Takes a [[client-session-state]] and closes it, cleaning up all resources."
  [client-session-state]
  ((client-session-state-close! client-session-state)))

(defn- make-client-session-state
  "Create a new [[client-session-state]]."
  [^ClientSession client-session ^ClientSessionFactory client-session-factory ^ServerLocator server-locator]
  (client-session-state client-session-state-get client-session
                        client-session-state-close! (fn []
                                                      (.close client-session)
                                                      (.close client-session-factory)
                                                      (.close server-locator))))

;; Create a new client session from a factory and some credentials.
(defn- create-client-session-1
  ^ClientSession [^ClientSessionFactory client-session-factory authentication-credentials]
  (if (r/is-a? username+password authentication-credentials)
    (.createSession client-session-factory
                    (username+password-username authentication-credentials)
                    (username+password-password authentication-credentials)
                    false
                    true
                    true
                    false
                    ActiveMQClient/DEFAULT_ACK_BATCH_SIZE)
    (.createSession client-session-factory)))

(defn transport-configs->server-locator
  "Takes an ordered sequence
  of [[org.apache.activemq.artemis.api.core.TransportConfiguration]]s and
  returns a [[org.apache.activemq.artemis.api.core.client.ServerLocator]].

  If `transport-configs` defines more than one connection, a high availability
  server locator will be created, using all connections in the order they appear
  in `transport-configs`. Otherwise, it will assume only one connection."
  ^org.apache.activemq.artemis.api.core.client.ServerLocator
  [transport-configs]
  (let [transport-configs* (into-array transport-configs)]
    (if (= 1 (count transport-configs*))
      (ActiveMQClient/createServerLocatorWithoutHA transport-configs*)
      (ActiveMQClient/createServerLocatorWithHA transport-configs*))))

(defn create-client-session
  "Create a new [[ClientSession]] from one of the predefined strategies. Does NOT
  start the session! `authentication-credentials` defines if and how
  authentication is performed and must be a valid [[authentication-credentials]]
  map."
  [connection-strategy authentication-credentials]
  (let [transport-configs (connection-strategy->transport-configs connection-strategy)
        ^ServerLocator locator (transport-configs->server-locator transport-configs)
        ^ClientSessionFactory factory (.createSessionFactory locator)
        ^ClientSession client-session (create-client-session-1 factory
                                                               authentication-credentials)]
    (make-client-session-state client-session factory locator)))
