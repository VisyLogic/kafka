FROM maven:3.9.9-eclipse-temurin-21 AS build
WORKDIR /build
COPY application/pom.xml ./pom.xml
COPY application/src ./src
COPY gui ./src/main/resources/web
RUN MAVEN_OPTS='-Xmx384m -XX:ActiveProcessorCount=2' mvn -B -q package

FROM eclipse-temurin:21-jre-jammy
RUN groupadd --system eventflow && useradd --system --gid eventflow eventflow
WORKDIR /app
COPY --from=build /build/target/eventflow-1.0.0.jar /app/eventflow.jar
USER eventflow
ENV JAVA_TOOL_OPTIONS="-Xms32m -Xmx192m -XX:ActiveProcessorCount=2"
ENTRYPOINT ["java","-jar","/app/eventflow.jar"]
