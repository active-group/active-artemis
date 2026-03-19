(ns active.artemis.impl.connection-strategy
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

(defn make-remote-host-strategy [uri]
  (strategy-remote-host strategy-remote-host-uri uri))

(r/def-record strategy-remote-host+port
  :extends connection-strategy
  [strategy-remote-host+port-host :- realm/string
   strategy-remote-host+port-port :- realm/string])

(defn make-remote-host+port-strategy [host port]
  (strategy-remote-host+port strategy-remote-host+port-host host
                             strategy-remote-host+port-port port))

(r/def-record strategy-same-vm
  :extends connection-strategy
  [strategy-same-vm-server-id :- realm/string])

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

(defn get-client-session-state [css]
  (client-session-state-get css))

(defn close-client-session-state! [css]
  ((client-session-state-close! css)))

(defn- make-client-session-state [^ClientSession client-session ^ClientSessionFactory client-session-factory ^ServerLocator server-locator]
  (client-session-state client-session-state-get client-session
                        client-session-state-close! (fn []
                                                      (.close client-session)
                                                      (.close client-session-factory)
                                                      (.close server-locator))))

(defn- create-remote-host-client-session [uri credentials]
  (let [^ServerLocator locator (ActiveMQClient/createServerLocator uri)
        ^ClientSessionFactory factory (.createSessionFactory locator)
        ^ClientSession client-session (if (r/is-a? username+password credentials)
                                        (.createSession factory
                                                        (username+password-username credentials)
                                                        (username+password-password credentials)
                                                        false
                                                        true
                                                        true
                                                        false
                                                        ActiveMQClient/DEFAULT_ACK_BATCH_SIZE)
                                        (.createSession factory))]
    (make-client-session-state client-session factory locator)))

(defn- create-remote-host+port-client-session
  [host port credentials]
  (let [params {"host" host
                "port" port}
        ^TransportConfiguration transport-cfg (TransportConfiguration. (.getName NettyConnectorFactory) params)
        ^ServerLocator locator (ActiveMQClient/createServerLocatorWithoutHA (into-array [transport-cfg]))
        ^ClientSessionFactory factory (.createSessionFactory locator)
        ^ClientSession client-session (if (r/is-a? username+password credentials)
                                        (.createSession factory
                                                        (username+password-username credentials)
                                                        (username+password-password credentials)
                                                        false
                                                        true
                                                        true
                                                        false
                                                        ActiveMQClient/DEFAULT_ACK_BATCH_SIZE)
                                        (.createSession factory))]
    (make-client-session-state client-session factory locator)))

#_(defn create-same-vm-client-session
    [server-id credentials]
    (let [params {"serverid" server-id}
          transport-cfg (TransportConfiguration. (.getName InVMConnectorFacotry) params)]))

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

    (r/is-a? strategy-same-vm
             connection-strategy)
    #_(create-same-vm-client-session (strategy-same-vm-server-id connection-strategy)
                                     credentials)
    (throw (UnsupportedOperationException. "not implemented"))))
