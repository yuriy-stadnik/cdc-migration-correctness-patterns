FROM gradle:8.7-jdk17 AS build

WORKDIR /workspace/local/cloud/lambda-consumer

COPY transaction-protocol /workspace/transaction-protocol
COPY transaction-apply /workspace/transaction-apply
COPY db /workspace/db
COPY local/cloud/lambda-consumer/settings.gradle local/cloud/lambda-consumer/build.gradle ./
COPY local/cloud/lambda-consumer/src ./src

RUN gradle --no-daemon shadowJar

FROM eclipse-temurin:17-jre

WORKDIR /app

COPY --from=build /workspace/local/cloud/lambda-consumer/build/libs/local-lambda-consumer-all.jar /app/local-lambda-consumer.jar

CMD ["java", "-jar", "/app/local-lambda-consumer.jar"]
