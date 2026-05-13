FROM apache/airflow:2.10.5-python3.11

USER root

# Install Spark 3.5.8 (prebuilt with Hadoop 3)
ENV SPARK_VERSION=3.5.8
ENV SPARK_HOME=/opt/spark

RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jdk-headless \
    wget \
    && rm -rf /var/lib/apt/lists/*

RUN wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz \
    && tar -xzf spark-${SPARK_VERSION}-bin-hadoop3.tgz -C /opt \
    && mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 ${SPARK_HOME} \
    && rm spark-${SPARK_VERSION}-bin-hadoop3.tgz

ENV PATH="${SPARK_HOME}/bin:${PATH}"

# Download required JARs for Spark
RUN mkdir -p ${SPARK_HOME}/jars

# MySQL Connector
RUN wget -q https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.4.0/mysql-connector-j-8.4.0.jar \
    -O ${SPARK_HOME}/jars/mysql-connector-j-8.4.0.jar

# Iceberg Spark Runtime
RUN wget -q https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.2/iceberg-spark-runtime-3.5_2.12-1.5.2.jar \
    -O ${SPARK_HOME}/jars/iceberg-spark-runtime-3.5_2.12-1.5.2.jar

# Hadoop AWS
RUN wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    -O ${SPARK_HOME}/jars/hadoop-aws-3.3.4.jar

# MinIO mc for bucket creation
RUN wget -q https://dl.min.io/client/mc/release/linux-amd64/mc \
    -O /usr/local/bin/mc && chmod +x /usr/local/bin/mc

USER airflow

# Install Python requirements
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
