FROM gradle:8.7-jdk17 AS build
WORKDIR /workspace
COPY transaction-protocol ./transaction-protocol
COPY transaction-projector ./transaction-projector
RUN gradle -p transaction-projector --no-daemon fatJar

FROM flink:1.19.1-scala_2.12-java17
RUN mkdir -p /opt/flink/checkpoints && chown flink:flink /opt/flink/checkpoints
COPY --from=build /workspace/transaction-projector/build/libs/transaction-projector-all.jar /opt/flink/usrlib/transaction-projector-all.jar
