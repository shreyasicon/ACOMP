# locust/Dockerfile
#
# ACOMP load generator image. Bundles Locust with the custom locustfile.py
# that exports Prometheus metrics on port 9646 for the ACOMP Collector.

FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY locustfile.py .

# 8089 = Locust's own web UI (optional, for manual observation)
# 9646 = Prometheus metrics endpoint exposed by locustfile.py's start_http_server
EXPOSE 8089 9646

ENTRYPOINT ["locust", "-f", "locustfile.py"]
