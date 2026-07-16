from prometheus_client import Counter, Histogram

prediction_requests = Counter(
    "prediction_requests_total",
    "Total prediction requests"
)

fraud_predictions = Counter(
    "fraud_predictions_total",
    "Total fraud predictions"
)

normal_predictions = Counter(
    "normal_predictions_total",
    "Total normal predictions"
)

prediction_latency = Histogram(
    "prediction_latency_seconds",
    "Prediction latency in seconds"
)