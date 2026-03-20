# `active-artemis`

Thin Clojure-wrapper around `org.apache.activemq/artemis-core-client` that only
speaks core.

## Example

Start the docker composition

```
$ docker compose up
```

Then, start the example app. It will publish a few messages

```
$ clj -M -m active.artemis.core
```

