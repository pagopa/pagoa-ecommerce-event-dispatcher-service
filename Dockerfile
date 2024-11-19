FROM openjdk:17-jdk@sha256:528707081fdb9562eb819128a9f85ae7fe000e2fbaeaf9f87662e7b3f38cb7d8 as build
WORKDIR /workspace/app

RUN microdnf install git

COPY mvnw .
COPY .mvn .mvn
COPY pom.xml .

RUN ./mvnw validate
RUN ./mvnw dependency:copy-dependencies
# RUN ./mvnw dependency:go-offline

COPY src src
COPY api-spec api-spec
COPY eclipse-style.xml eclipse-style.xml
# COPY api-spec api-spec
RUN ./mvnw install -DskipTests # --offline
RUN mkdir target/extracted && java -Djarmode=layertools -jar target/*.jar extract --destination target/extracted

FROM openjdk:17-slim@sha256:aaa3b3cb27e3e520b8f116863d0580c438ed55ecfa0bc126b41f68c3f62f9774

RUN addgroup --system user && adduser --ingroup user --system user
USER user:user

WORKDIR /app/

ARG EXTRACTED=/workspace/app/target/extracted

# OpenTelemetry agent
ADD --chown=user https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/download/v2.1.0/opentelemetry-javaagent.jar .

COPY --from=build --chown=user ${EXTRACTED}/dependencies/ ./
RUN true
COPY --from=build --chown=user ${EXTRACTED}/spring-boot-loader/ ./
RUN true
COPY --from=build --chown=user ${EXTRACTED}/snapshot-dependencies/ ./
RUN true
COPY --from=build --chown=user ${EXTRACTED}/application/ ./
RUN true


ENTRYPOINT ["java","-javaagent:opentelemetry-javaagent.jar","--enable-preview","org.springframework.boot.loader.JarLauncher"]
