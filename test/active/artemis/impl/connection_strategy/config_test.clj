(ns active.artemis.impl.connection-strategy.config-test
  (:require [active.artemis.impl.connection-strategy.config :as sut]
            [active.clojure.config :as config]
            [clojure.test :as t]))

(t/deftest host+port-schema-test
  (let [m {:host "some-remote"
           :port 61617}
        config (config/make-configuration sut/host+port-schema [] m)]
    (t/is (= "some-remote"
             (config/access config sut/host-setting)))
    (t/is (= 61617
             (config/access config sut/port-setting))))
  (t/testing "fills correct defaults"
    (let [m {}
          config (config/make-configuration sut/host+port-schema [] m)]
      (t/is (= "localhost"
               (config/access config sut/host-setting)))
      (t/is (= 61616
               (config/access config sut/port-setting))))))

(t/deftest multi-host-schema-test
  (let [m [{:host "some-remote"
            :port 61617}
           {:host "other-host"
            :port 61618}]
        config (config/make-configuration sut/multi-schema [] m)]
    (t/is (= "some-remote"
             (config/access config sut/host-setting 0)))
    (t/is (= 61617
             (config/access config sut/port-setting 0)))
    (t/is (= "other-host"
             (config/access config sut/host-setting 1)))
    (t/is (= 61618
             (config/access config sut/port-setting 1)))))
