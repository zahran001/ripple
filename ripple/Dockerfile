FROM eclipse-temurin:21-jre-alpine AS runtime

RUN apk add --no-cache curl

WORKDIR /app

ARG JAR_FILE=target/ripple-0.1.0-SNAPSHOT.jar
COPY ${JAR_FILE} app.jar

RUN mkdir -p /tmp/ripple-chronicle

EXPOSE 8080

ENV JAVA_OPTS="\
  --add-opens java.base/java.lang=ALL-UNNAMED \
  --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens java.base/java.io=ALL-UNNAMED \
  --enable-preview"

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar /app/app.jar"]
