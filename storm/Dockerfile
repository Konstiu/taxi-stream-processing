# --- Stage 1: build your Java topology ---
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /app
COPY pom.xml .
COPY src ./src
RUN mvn -q -DskipTests package

# --- Stage 2: Storm runtime (Nimbus/Supervisor/UI) ---
FROM storm:2.8.2-jre17
COPY storm.yaml /conf/storm.yaml
#COPY --from=build /app/target/*-jar-with-dependencies.jar /topology.jar
COPY --from=build /app/target/topology.jar /topology.jar
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
