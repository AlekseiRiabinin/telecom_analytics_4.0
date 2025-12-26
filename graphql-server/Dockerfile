FROM eclipse-temurin:17-jre

WORKDIR /app

COPY target/scala-2.13/graphql-cre.jar app.jar

EXPOSE 8088

ENTRYPOINT ["java", "-jar", "app.jar"]
