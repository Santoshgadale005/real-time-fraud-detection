from http.server import HTTPServer, BaseHTTPRequestHandler
import random, time, threading

throughput = 100
latency = 50
alerts = 0
high_risk = 0

class MetricsHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        global throughput, latency, alerts, high_risk
        if self.path == '/metrics':
            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            
            throughput += random.randint(-10, 10)
            if throughput < 10: throughput = 10
            
            latency += random.randint(-5, 5)
            if latency < 20: latency = 20
            
            alerts += random.randint(0, 3)
            high_risk += random.randint(0, 1)
            
            res = f"""# HELP spark_streaming_processed_rows_total processed rows
# TYPE spark_streaming_processed_rows_total counter
spark_streaming_processed_rows_total {throughput * time.time()}
# HELP spark_streaming_prediction_latency_ms latency
# TYPE spark_streaming_prediction_latency_ms gauge
spark_streaming_prediction_latency_ms {latency}
# HELP mongodb_fraud_alerts_total fraud alerts
# TYPE mongodb_fraud_alerts_total counter
mongodb_fraud_alerts_total {alerts}
# HELP mongodb_high_risk_alerts_total high risk alerts
# TYPE mongodb_high_risk_alerts_total counter
mongodb_high_risk_alerts_total {high_risk}
"""
            self.wfile.write(res.encode('utf-8'))
        else:
            self.send_response(404)
            self.end_headers()

def run():
    server = HTTPServer(('0.0.0.0', 8001), MetricsHandler)
    server.serve_forever()

if __name__ == '__main__':
    run()
