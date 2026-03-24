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

  * A plain [[strategy-remote-host]] that describes the attempt to connect to a
  remote Artemis instance using a plain uri. Create such a strategy
  via [[make-remote-host-strategy]].

  * A more complex [[strategy-remote-host+port]] that describes the attempt to
  connect to a remote Artemis instance using a specific host and port. Create
  such a strategy via [[make-remote-host+port-strategy]].

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
   [org.apache.activemq.artemis.core.remoting.impl.netty NettyConnectorFactory]))

(r/def-record connection-strategy [])

(r/def-record strategy-remote-host
  :extends connection-strategy
  [strategy-remote-host-uri :- realm/string])

(defn make-remote-host-strategy
  "Takes a `uri` (string) and returns a [[strategy-remote-host]] for that `uri`."
  [uri]
  (strategy-remote-host strategy-remote-host-uri uri))

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


(r/def-record strategy-multi-remote-host+port
  :extends connection-strategy
  [strategy-multi-remote-host+port-configs :- (realm/sequence-of strategy-remote-host+port)])

(defn make-multi-remote-host+port-strategy
  [& remote-host-strategies]
  (strategy-multi-remote-host+port strategy-multi-remote-host+port-configs
                                   remote-host-strategies))

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

(defn- create-remote-host-client-session
  [uri credentials]
  (let [^ServerLocator locator (ActiveMQClient/createServerLocator uri)
        ^ClientSessionFactory factory (.createSessionFactory locator)
        ^ClientSession client-session (create-client-session-1 factory
                                                               credentials)]
    (make-client-session-state client-session factory locator)))

(defn- create-remote-host+port-client-session
  [host port credentials]
  (let [params {"host" host
                "port" port}
        ^TransportConfiguration transport-cfg (TransportConfiguration. (.getName NettyConnectorFactory) params)
        ^ServerLocator locator (ActiveMQClient/createServerLocatorWithoutHA (into-array [transport-cfg]))
        ^ClientSessionFactory factory (.createSessionFactory locator)
        ^ClientSession client-session (create-client-session-1 factory
                                                               credentials)]
    (make-client-session-state client-session factory locator)))

;; NOTE: We make the assunmption that all host+port entries use the same
;; credentials to connect to. Otherwise, we would need to work this out without
;; the provided server locator. Let's see if we even need it.

;; NOTE: The servers are tried in the sequence they appear in `host+port-seq`.
(defn create-multi-remote-host+port-client-session
  [host+port-seq credentials]
  (let [transport-cfgs ;; TODO (Marco): Type hints.
        (mapv (fn [host+port]
                (TransportConfiguration. (.getName NettyConnectorFactory)
                                         {"host" (strategy-remote-host+port-host host+port)
                                          "port" (strategy-remote-host+port-port host+port)}))
              host+port-seq)
        ^ServerLocator locator (ActiveMQClient/createServerLocatorWithHA (into-array transport-cfgs))
        ^ClientSessionFactory factory (.createSessionFactory locator)
        ^ClientSession client-session (create-client-session-1 factory
                                                               credentials)]
    (make-client-session-state client-session factory locator)))

(defn create-client-session
  "Create a new [[ClientSession]] from one of the predefined strategies. Does NOT
  start the session! `authentication-credentials` defines if and how
  authentication is performed and must be a valid [[authentication-credentials]]
  map."
  [connection-strategy authentication-credentials]
  (cond
    (r/is-a? strategy-remote-host
             connection-strategy)
    (create-remote-host-client-session (strategy-remote-host-uri connection-strategy)
                                       authentication-credentials)

    (r/is-a? strategy-remote-host+port
             connection-strategy)
    (create-remote-host+port-client-session (strategy-remote-host+port-host connection-strategy)
                                            (strategy-remote-host+port-port connection-strategy)
                                            authentication-credentials)

    (r/is-a? strategy-multi-remote-host+port
             connection-strategy)
    (create-multi-remote-host+port-client-session (strategy-multi-remote-host+port-configs connection-strategy)
                                                  authentication-credentials)
    
    :else
    (throw (UnsupportedOperationException. "not implemented"))))
