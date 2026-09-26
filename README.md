# n8n_kafka_api

GitHub PR Webhook을 수신해 Kafka(`github.pr.events`)에 저장하는 Spring Boot Receiver.
집 서버의 n8n이 Kafka를 consume해 PR 리뷰와 Study Guide를 생성한다.

전체 설계: [docs/N8N_KAFKA_PR_REVIEW_STUDY_GUIDE_SPEC.md](docs/N8N_KAFKA_PR_REVIEW_STUDY_GUIDE_SPEC.md)

## Stack

- Java 21, Spring Boot 4.1, Spring Kafka
- Gradle (Kotlin DSL)
- Docker, Docker Compose (Kafka KRaft single broker)
- GitHub Actions (CI/CD), GHCR

## 구조

```text
.
├── .github/workflows/ci-cd.yml   # build/test → 이미지 publish → OCI 배포
├── docs/                         # 설계 명세
├── src/main/java/com/owencity/n8nkafka/
│   ├── N8nKafkaApiApplication.java
│   ├── webhook/                  # (Phase 2) Webhook 수신, 서명 검증, payload 파싱
│   ├── event/                    # (Phase 3) GithubPrEvent, Kafka producer
│   └── config/                   # 설정 properties
├── src/main/resources/application.yml
├── Dockerfile
└── compose.yaml                  # (Phase 1) Kafka + receiver
```

단일 모듈 모놀리스, 기능 단위 패키지를 사용한다. 패키지는 해당 Phase에서 필요할 때 만든다.

## 로컬 실행

```bash
./gradlew build          # 빌드 + 테스트
./gradlew bootRun        # 실행 (기본 Kafka: localhost:9092)
docker build -t n8n-kafka-api .
```

## 환경변수

| 이름 | 기본값 | 설명 |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker 주소 |

## CI/CD

`.github/workflows/ci-cd.yml`

| Job | 실행 조건 | 내용 |
|---|---|---|
| `build` | 모든 PR, main push | `./gradlew build`, Docker 이미지 빌드 검증 |
| `publish` | main push | `ghcr.io/owencity/n8n_kafka_api:{latest, sha-xxxx}` push (amd64/arm64) |
| `deploy` | main push + `DEPLOY_ENABLED=true` | OCI에 SSH 접속 후 `docker compose pull app && up -d app` |

### 배포 활성화

GitHub 저장소 Settings에서 설정한다.

- Variables: `DEPLOY_ENABLED=true`, `DEPLOY_PATH` (서버의 compose.yaml 위치)
- Secrets: `OCI_HOST`, `OCI_USER`, `OCI_SSH_KEY` (배포 전용 private key), `OCI_KNOWN_HOSTS` (`ssh-keyscan <host>` 결과)

OCI 서버 준비:

1. Docker, Docker Compose 설치
2. GHCR 패키지는 기본 private이므로 `read:packages` 권한 토큰으로 `docker login ghcr.io` 1회 실행
3. `DEPLOY_PATH`에 `compose.yaml`과 `.env` 배치 (`app` 서비스 이미지: `ghcr.io/owencity/n8n_kafka_api:latest`)

<!-- n8n review flow test: close this PR without merging -->
<!-- review flow event 214818 -->
