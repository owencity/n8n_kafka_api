# syntax=docker/dockerfile:1

# 빌드 단계는 러너의 네이티브 플랫폼에서 실행한다.
# jar는 플랫폼 독립적이므로 arm64 이미지를 만들 때도 Gradle을 에뮬레이션하지 않는다.
FROM --platform=$BUILDPLATFORM eclipse-temurin:21-jdk AS build
WORKDIR /workspace
COPY gradlew settings.gradle.kts build.gradle.kts ./
COPY gradle gradle
RUN ./gradlew dependencies --no-daemon > /dev/null
COPY src src
RUN ./gradlew bootJar --no-daemon -x test

FROM eclipse-temurin:21-jre
WORKDIR /app
RUN groupadd --system app && useradd --system --gid app app
COPY --from=build /workspace/build/libs/*.jar app.jar
USER app
EXPOSE 8080
# JVM 옵션은 JAVA_TOOL_OPTIONS 환경변수로 전달한다.
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
