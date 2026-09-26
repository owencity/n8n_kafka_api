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
├── scripts/kafka-smoke-test.sh   # Kafka produce/consume, offset 재개 확인
├── Dockerfile
├── compose.yaml                  # Kafka(KRaft) + topic 생성 + receiver
└── .env.example
```

단일 모듈 모놀리스, 기능 단위 패키지를 사용한다. 패키지는 해당 Phase에서 필요할 때 만든다.

## 로컬 실행

```bash
docker compose up -d --build        # kafka → kafka-init(topic 생성) → app 순서로 기동
bash scripts/kafka-smoke-test.sh    # Kafka 동작 확인 (PASS 출력)
curl localhost:8080/actuator/health
docker compose down                 # 종료 (데이터 유지). 데이터까지 지우려면 -v
```

앱만 IDE/Gradle로 실행할 때는 `docker compose up -d kafka kafka-init` 후 `./gradlew bootRun` (Kafka: `localhost:9092`).

### Compose 구성

| 서비스 | 역할 |
|---|---|
| `kafka` | apache/kafka 4.3.1, KRaft single broker. 데이터는 `kafka-data` 볼륨 |
| `kafka-init` | `github.pr.events` topic 생성 (3 partitions, retention 7일) 후 종료 |
| `app` | Webhook Receiver (`ghcr.io/owencity/n8n_kafka_api`, 로컬은 `--build`로 직접 빌드) |

Kafka listener:

- `INTERNAL` `kafka:19092`: compose 네트워크 내부(app)용
- `EXTERNAL` `${KAFKA_EXTERNAL_HOST}:9092`: 호스트/사설망(집 n8n)용. 기본은 `127.0.0.1`에만 바인딩

topic 자동 생성은 꺼져 있다. 오타난 topic 이름으로 접근하면 에러가 난다.

## 환경변수

앱:

| 이름 | 기본값 | 설명 |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker 주소 (compose에서는 `kafka:19092`) |

Compose (`.env.example` 참고):

| 이름 | 기본값 | 설명 |
|---|---|---|
| `KAFKA_EXTERNAL_HOST` | `localhost` | EXTERNAL listener가 광고하는 주소 (OCI: Tailscale IP) |
| `KAFKA_EXTERNAL_BIND` | `127.0.0.1` | 9092 바인딩 IP. 공인망(`0.0.0.0`)에 열지 않는다 |
| `APP_BIND` | `127.0.0.1` | 8080 바인딩 IP |
| `APP_TAG` | `latest` | app 이미지 태그 |

## CI/CD

`.github/workflows/ci-cd.yml`

| Job | 실행 조건 | 내용 |
|---|---|---|
| `build` | 모든 브랜치 push | `./gradlew build`, Docker 이미지 빌드 검증 |
| `publish` | main push | `ghcr.io/owencity/n8n_kafka_api:{latest, sha-xxxx}` push (amd64/arm64) |
| `deploy` | main push + `DEPLOY_ENABLED=true` | OCI에 SSH 접속 후 `docker compose pull app && up -d app` |

### 브랜치 / PR 운영

기능 전체를 하나의 브랜치에서 개발하고, 완성된 뒤 PR을 한 번 만든다. 그 PR이 n8n 리뷰와 Study Guide의 대상이 된다.
커밋에는 Study Unit 태그를 붙인다: `[study:<unit>] <type>: <내용>` (명세 7장)

### 배포 활성화

GitHub 저장소 Settings에서 설정한다.

- Variables: `DEPLOY_ENABLED=true`, `DEPLOY_PATH` (서버의 compose.yaml 위치)
- Secrets: `OCI_HOST`, `OCI_USER`, `OCI_SSH_KEY` (배포 전용 private key), `OCI_KNOWN_HOSTS` (`ssh-keyscan <host>` 결과)

OCI 서버 준비:

1. Docker, Docker Compose 설치
2. GHCR 패키지는 기본 private이므로 `read:packages` 권한 토큰으로 `docker login ghcr.io` 1회 실행
3. `DEPLOY_PATH`에 `compose.yaml`과 `.env` 배치 (`app` 서비스 이미지: `ghcr.io/owencity/n8n_kafka_api:latest`)
