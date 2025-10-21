FROM debezium/connect:2.5.4.Final

LABEL maintainer="Aleksei Riabinin <aleksei.riabinin@yahoo.com>"
LABEL description="Debezium Connect with updated MSSQL JDBC Driver"

USER root

# Replace the built-in Java 8 driver with our Java 11 driver
RUN rm -f /kafka/connect/debezium-connector-sqlserver/mssql-jdbc-*.jar
RUN rm -f /kafka/connect/debezium-connector-jdbc/mssql-jdbc-*.jar

# Copy our newer JDBC driver
COPY mssql-jdbc-13.2.1.jre11.jar /kafka/connect/debezium-connector-sqlserver/
COPY mssql-jdbc-13.2.1.jre11.jar /kafka/connect/debezium-connector-jdbc/

# Copy debug log configuration
COPY connect-log4j-debug.properties /kafka/config/connect-log4j.properties

# Set proper ownership
RUN chown -R kafka:kafka /kafka/connect/debezium-connector-sqlserver/ /kafka/connect/debezium-connector-jdbc/ /kafka/config/

# Verify the files are in place
RUN echo "=== Verification ===" && \
    ls -la /kafka/connect/debezium-connector-sqlserver/mssql-jdbc* && \
    ls -la /kafka/connect/debezium-connector-jdbc/mssql-jdbc* && \
    ls -la /kafka/config/connect-log4j.properties

USER kafka

HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8083/ || exit 1
