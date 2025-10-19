FROM debezium/connect:2.5.4.Final

LABEL maintainer="Aleksei Riabinin <aleksei.riabinin@yahoo.com>"
LABEL description="Debezium Connect with MSSQL JDBC Driver"

USER root

# Copy JDBC driver to multiple possible locations to ensure it's found
COPY mssql-jdbc-13.2.1.jre11.jar /kafka/libs/mssql-jdbc-13.2.1.jre11.jar
COPY mssql-jdbc-13.2.1.jre11.jar /usr/share/java/kafka/mssql-jdbc-13.2.1.jre11.jar

# Set proper ownership
RUN chown kafka:kafka /kafka/libs/mssql-jdbc-13.2.1.jre11.jar && \
    chown kafka:kafka /usr/share/java/kafka/mssql-jdbc-13.2.1.jre11.jar

# Create symlink to ensure it's in classpath
RUN ln -sf /kafka/libs/mssql-jdbc-13.2.1.jre11.jar /usr/share/java/kafka/mssql-jdbc.jar

# Verify all locations
RUN echo "=== Verification ===" && \
    echo "/kafka/libs:" && ls -la /kafka/libs/mssql-jdbc* 2>/dev/null || echo "Not in /kafka/libs" && \
    echo "/usr/share/java/kafka:" && ls -la /usr/share/java/kafka/mssql-jdbc* 2>/dev/null || echo "Not in /usr/share/java/kafka" && \
    find / -name '*mssql-jdbc*' -type f 2>/dev/null | head -10

USER kafka

HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8083/ || exit 1
