---
pr: TBD
url: https://github.com/llm-d/llm-d-async/pull/TBD
author: mstandley-tempus
date: 2026-11-16
---
Prometheus gates (`prometheus-saturation`, `prometheus-budget`, `prometheus-query`) now log a startup warning when `prometheus-ca-cert` is set without `prometheus-bearer-token-file` while `prometheus-url` targets Google Managed Prometheus (`monitoring.googleapis.com`). With only a CA cert, the explicit transport short-circuits GMP's Application Default Credentials, so gate queries get HTTP 401 and gates silently fall back to their configured fallback (default `0.0`, closed). The warning explains the two fixes: omit both flags to keep ADC, or also set `prometheus-bearer-token-file`. The same guidance is now documented on the `prometheus-ca-cert` flag.
