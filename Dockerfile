# Expect pre-built fat jar: mvn -B -ntp package
FROM eclipse-temurin:21-jre-jammy
WORKDIR /app
COPY target/town-power-cluster.jar /app/town-power-cluster.jar
ENV HTTP_BIND_HOST=0.0.0.0 \
	PEKKO_BIND_HOSTNAME=0.0.0.0 \
	JAVA_OPTS="-Xms128m -Xmx384m"
EXPOSE 2551 9551
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/town-power-cluster.jar ${NODE_PORT:-2551}"]
