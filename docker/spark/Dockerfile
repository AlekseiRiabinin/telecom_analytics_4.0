# docker/spark/Dockerfile
FROM apache/spark:4.0.1

USER root

RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN curl -L -o /opt/spark/jars/clickhouse-jdbc-0.9.2.jar \
    https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.9.2/clickhouse-jdbc-0.9.2.jar

RUN mkdir -p /opt/spark/work-dir
WORKDIR /opt/spark/work-dir

COPY target/scala-2.13/spark-job-assembly-1.0.0.jar /opt/spark/work-dir/app.jar

COPY docker/entrypoint.sh /opt/spark/work-dir/
RUN chmod +x /opt/spark/work-dir/entrypoint.sh

USER spark

ENTRYPOINT ["/opt/spark/work-dir/entrypoint.sh"]
