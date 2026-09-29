from dagster import Backoff, Jitter, RetryPolicy

RETRY_POLICY = RetryPolicy(
    max_retries=3,
    delay=5,
    backoff=Backoff.EXPONENTIAL,
    jitter=Jitter.FULL,
)
