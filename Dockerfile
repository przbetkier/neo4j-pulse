FROM eclipse-temurin:23-jre-alpine-3.21

WORKDIR /app
RUN addgroup -S neo4j-pulse && adduser -S -G neo4j-pulse neo4j-pulse

COPY build/distributions/neo4j-pulse-1.0.zip /app
RUN unzip neo4j-pulse-1.0.zip && rm neo4j-pulse-1.0.zip

RUN chown -R neo4j-pulse:neo4j-pulse /app
USER neo4j-pulse

EXPOSE 4242
CMD ["neo4j-pulse-1.0/bin/neo4j-pulse"]