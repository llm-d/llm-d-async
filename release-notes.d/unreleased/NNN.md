---
pr: NNN
url: https://github.com/llm-d/llm-d-async/pull/NNN
author: sohom-cs
date: 2026-11-19
---
redis-sortedset: a request parked for retry when its processor shuts down gracefully, such as during a rolling upgrade, is no longer dispatched a second time once the old processor's claim lease expires.
