FROM gradle:8.7-jdk17 AS build

WORKDIR /app

COPY settings.gradle build.gradle ./
COPY src ./src

RUN gradle --no-daemon shadowJar

FROM eclipse-temurin:17-jre

WORKDIR /app

COPY --from=build /app/build/libs/local-lambda-consumer-all.jar /app/local-lambda-consumer.jar

CMD ["java", "-jar", "/app/local-lambda-consumer.jar"]
