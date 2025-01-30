FROM bitnami/spark:latest as jupyterlab

USER root

# Create the spark user and set permissions
RUN useradd -u 1001 -g root spark && \
    chown -R spark:root /opt/bitnami/spark && \
    chmod -R g+rwX /opt/bitnami/spark

COPY Requirements.txt /tmp/Requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r /tmp/Requirements.txt && \
    pip install --upgrade pip

# Set environment variables for Spark
ENV SPARK_HOME=/opt/bitnami/spark
ENV PATH=$SPARK_HOME/bin:$PATH
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH

# Set the working directory
WORKDIR /app

# Expose ports for Spark and JupyterLab
EXPOSE 8888

# Switch to the spark user
USER spark

# Start Spark master, Spark worker, and JupyterLab
CMD ["bash", "-c", "/opt/bitnami/scripts/spark/run.sh && \
     /opt/bitnami/scripts/spark-worker/run.sh &&]