FROM bitnami/spark:latest

USER root

# Install system dependencies required by pandas and numpy (IMPORTANT!)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        liblapack-dev \
        libblas-dev \
        gfortran \
        python3-dev \
        && rm -rf /var/lib/apt/lists/*

# Create the spark user and set permissions
RUN useradd -u 1001 -g root spark && \
    chown -R spark:root /opt/bitnami/spark && \
    chmod -R g+rwX /opt/bitnami/spark && \
    chown -R spark:root /opt/bitnami/spark/logs

COPY Requirements.txt /tmp/Requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r /tmp/Requirements.txt && \
    pip install --upgrade pip

# Set environment variables for Spark
ENV SPARK_HOME=/opt/bitnami/spark
ENV PATH=$SPARK_HOME/bin:$PATH
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH

# Expose ports for Spark and JupyterLab
EXPOSE 8888

# Switch to the spark user
USER spark

# Start Spark master, Spark worker, and JupyterLab
CMD ["bash", "-c", "/opt/bitnami/scripts/spark/run.sh && \
     /opt/bitnami/scripts/spark-worker/run.sh"]