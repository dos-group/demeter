FROM maven:3.8.3-openjdk-17-slim AS build
WORKDIR /demeter
# we want to leverage docker caching
COPY pom.xml /demeter
COPY optimizer/*.xml /demeter/optimizer/
RUN mvn dependency:go-offline
# now copy the rest and build the application
COPY . /demeter
RUN mvn package -DskipTests


FROM openjdk:17
WORKDIR /demeter
COPY --from=build /demeter/optimizer/target/optimizer-1.0.jar /demeter/
COPY --from=build /demeter/binaries /demeter/binaries/
COPY --from=build /demeter/optimizer/src/main/resources/kubernetes /demeter/kubernetes/
ENTRYPOINT ["java", "-jar", "optimizer-1.0.jar"]


