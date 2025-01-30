FROM bitnami/spark:latest

USER root

COPY Requirements.txt /tmp/Requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r /tmp/Requirements.txt