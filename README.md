# Goals

* Create robust, fault-tolerant concurrent systems.
* Actor model of concurrency.
* Routers for allowing a single actor to process multiple messages concurrently.
* (TODO) Prevent cycles in actor hierarchy.
* Actors are multiplexed onto coroutines.
* (TODO) Actors are multiplexed onto processes.
* Actor is a state machine that returns its next state behaviour.
* (TODO) Dead-letter handling.
* (TODO) Event-sourced persistence.
* (TODO) Dependency injection.
* (TODO) Rate-limiting
* Observability
    * Metrics
        * Inbox sizes
        * Memory usage
        * Latency
        * Message rates
    * Logging
