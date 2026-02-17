FROM openjdk:25-ea-jdk-slim
#COPY target/quarkus-app/quarkus-run.jar test.jar
COPY target/quarkus-app/ /app/
WORKDIR /app
ENTRYPOINT ["java", "-jar", "/app/quarkus-run.jar"]