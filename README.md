# Goals

* Create robust, fault-tolerant concurrent systems.
* Actor model of concurrency.
    * Routers for allowing a single actor to process multiple messages concurrently.
    * (TODO) Prevent cycles in actor hierarchy.
    * (TODO) Dead letter handling.
* Actors are multiplexed onto coroutines and processes.
* Actor is a state machine that returns its next state behaviour.
* Dead-letter handling.
* Event-sourced persistence.
* Dependency injection.
* Rate-limiting
* Observablity
    * Inbox sizes
    * Memory usage
    * Latency
    * Message rates
