FROM apache/spark:3.5.4

USER root

COPY jars/*.jar /opt/spark/jars/

RUN chown -R spark:spark /opt/spark/jars

USER spark
