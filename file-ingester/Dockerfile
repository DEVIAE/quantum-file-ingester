FROM eclipse-temurin:21-jre-alpine

LABEL maintainer="quantum-team"
LABEL description="Quantum File Ingester - Apache Camel file splitting service"

RUN addgroup -S quantum && adduser -S quantum -G quantum

WORKDIR /app

COPY target/quantum-file-ingester-*.jar app.jar

RUN mkdir -p /data/input && chown -R quantum:quantum /data

USER quantum

EXPOSE 8081

HEALTHCHECK --interval=30s --timeout=3s --retries=3 \
  CMD wget -qO- http://localhost:8081/actuator/health || exit 1

ENTRYPOINT ["java", \
  "-XX:+UseG1GC", \
  "-XX:MaxRAMPercentage=75.0", \
  "-XX:+ExitOnOutOfMemoryError", \
  "-Djava.security.egd=file:/dev/./urandom", \
  "-jar", "app.jar"]
