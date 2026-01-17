# -----------------------------
# Dockerfile: Jupyter + Java 11 + PySpark + GraphFrames
# -----------------------------

FROM jupyter/pyspark-notebook:latest

# Switch to root to install Java 11 and other dependencies
USER root

# Install Java 11 (headless) and clean up
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk-headless && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME (needed for Spark/GraphFrames)
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

# Install GraphFrames Python package
RUN pip install --no-cache-dir graphframes

# Switch back to default Jupyter user
USER jovyan

# Expose the Jupyter port (host port mapped in 'docker run')
EXPOSE 8888

# Default command is already jupyter notebook in the base image
