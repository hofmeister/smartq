SmartQ
======

A java task queing framework. Can have any backing store (just implement another TaskStore) but ships with In-Memory and Redis (for distributed queues)

Features
========

- Distributed when using Redis as backing store. No sharding support yet.
- Each task has a estimated duration - which allows SmartQ to estimate how long it will take to process the remaining queue.
- Built-in rate limiting - based on task type. This means that you can limit tasks of type "cpuintensive" to 2 (at a time) - and have no limit on task type "quick". ETA calculation will take rate limiting into account and calculate using the current concurrency setting ( Called consumers )
