(ns active.artemis.impl.connection-strategy.config
  "Schemas and settings for interfacing with [[active.clojure.config]]."
  (:require
   [active.clojure.config :as config]))

(def host-setting
  (config/setting :host
                  "Defines the host where the Artemis instance is accepts connections at."
                  (config/default-string-range "localhost")))

(def port-setting
  (config/setting :port
                  "Defines the port the Artemis instance is listening on."
                  (config/integer-between-range 0
                                                (int (Math/pow 2 16))
                                                61616)))

(def host+port-schema
  (config/schema "Schema that defines a host+port connection strategy configuration."
                 host-setting
                 port-setting))

(def multi-schema
  (config/sequence-schema "Schema that defines one or more host+port connection strategies."
                          host+port-schema))

