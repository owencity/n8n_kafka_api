# n8n + Kafka PR Review / Study Guide PoC 개발 명세

> Claude Code가 이 문서를 기준으로 `n8n_kafka` PoC를 구현할 수 있도록 아키텍처, 이벤트 흐름, PR/Study Unit 규칙, Gemini 호출 정책, 장애 처리 원칙을 정의한다.

## 1. 프로젝트 목표

이 프로젝트의 목적은 GitHub PR 이벤트를 안정적으로 수집하고, PR 전체를 한 번의 Gemini 호출로 리뷰하면서 **Study Unit별 코드리뷰 + 학습 가이드**를 생성하는 것이다.

핵심 목표:

- GitHub Webhook 이벤트가 집의 n8n 서버 장애 때문에 유실되지 않도록 한다.
- OCI의 Spring Boot가 GitHub Webhook을 수신하고 Kafka에 이벤트를 저장한다.
- 집의 n8n은 Kafka Consumer로 동작하며 이벤트를 consume한다.
- 개발자는 기능을 전부 구현한 뒤 하나의 PR을 생성할 수 있다.
- 개발 중 남긴 여러 commit을 `Study Unit`으로 그룹화한다.
- Study Unit이 여러 개여도 **Gemini API는 기본적으로 PR당 1회만 호출한다.**
- Gemini는 PR 전체 리뷰와 Study Unit별 학습 가이드를 한 번에 생성한다.
- 학습 항목은 임의의 개수 제한을 두지 않는다.
- 실제 변경사항과 직접 연관된 학습 항목은 가능한 한 누락하지 않는다.
- Gemini를 사용할 수 없는 경우에만 Ollama를 fallback으로 사용한다.

---

## 2. 전체 아키텍처

```text
GitHub
  |
  | Webhook
  v
OCI
+-----------------------------+
| Spring Boot Webhook Receiver|
|             |               |
|             | produce       |
|             v               |
|           Kafka             |
|      github.pr.events       |
+-------------+---------------+
              ^
              | consume
              | private network
              |
Home Server
+-----------------------------+
| n8n                         |
|  Kafka Trigger              |
|      |                      |
|      v                      |
| GitHub API                  |
|      |                      |
|      v                      |
| Study Unit Grouping         |
|      |                      |
|      v                      |
| Gemini Primary              |
|      |                      |
|      +--> GitHub Comment    |
|      +--> Slack Study Guide |
|                             |
| Gemini unavailable          |
|      |                      |
|      v                      |
| Ollama Fallback             |
+-----------------------------+
```

---

## 3. 컴포넌트 책임

### GitHub

초기 PoC에서 처리할 이벤트:

- `pull_request.opened`
- `pull_request.synchronize`
- `pull_request.reopened`

GitHub는 OCI의 Spring Boot Webhook Receiver로 이벤트를 전송한다.

### Spring Boot Webhook Receiver

Spring Boot는 **Webhook 수신 + 검증 + Kafka publish**까지만 담당한다.

AI 리뷰, GitHub diff 조회, Study Guide 생성 등의 작업은 하지 않는다.

```text
POST /webhooks/github
    |
    v
X-Hub-Signature-256 검증
    |
    v
GitHub event/action 확인
    |
    v
필요 metadata 추출
    |
    v
Kafka publish
    |
    v
Broker ACK 확인
    |
    v
2xx 응답
```

원칙:

- Kafka publish 성공을 확인하기 전에 GitHub에 성공 응답을 반환하지 않는다.
- GitHub secret은 환경변수 또는 secret manager를 사용한다.
- 로그에 secret/token 전체 값을 출력하지 않는다.
- Webhook Receiver는 가능한 한 가볍게 유지한다.

---

## 4. Kafka Event Schema

Kafka에는 전체 PR diff를 넣지 않는다.

Kafka는 **이벤트 전달과 보존** 역할만 담당하며, 실제 최신 PR 정보는 n8n이 GitHub API를 통해 조회한다.

### Produce 계약 (고정)

Spring Boot producer와 n8n consumer 사이의 메시지 계약이다. **반드시 아래 형태로 produce한다.**

```json
{
  "deliveryId": "github-delivery-id",
  "event": "pull_request",
  "action": "synchronize",
  "repo": "owencity/n8n_kafka",
  "prNumber": 4,
  "headSha": "abc123..."
}
```

| 필드 | 타입 | 출처 |
|---|---|---|
| `deliveryId` | string | `X-GitHub-Delivery` 헤더 |
| `event` | string | `X-GitHub-Event` 헤더 |
| `action` | string | payload `action` |
| `repo` | string | payload `repository.full_name` |
| `prNumber` | number (정수) | payload `pull_request.number` |
| `headSha` | string | payload `pull_request.head.sha` |

규칙:

- 6개 필드 모두 필수이며 null/빈 문자열을 허용하지 않는다.
- 필드를 추가/삭제/이름 변경하지 않는다. 변경이 필요하면 n8n consumer와 먼저 합의한다.
- value는 순수 JSON만 담는다. Java 타입 정보 헤더(`__TypeId__`)에 consumer가 의존하지 않도록 한다.
- 구현: `GithubPrEvent` record, 계약 테스트 `GithubPrEventContractTest`, fixture `src/test/resources/contract/github-pr-event.json`

권장 Topic:

```text
github.pr.events
```

권장 Key:

```text
{repo}:{prNumber}
```

예:

```text
owencity/n8n_kafka:4
```

같은 PR 이벤트가 가능한 한 동일 partition에서 순서를 유지하도록 하기 위함이다.

---

## 5. n8n의 역할

n8n은 Kafka Topic을 구독하는 Consumer다.

Webhook처럼 Kafka가 n8n URL로 메시지를 보내는 방식이 아니다.

```text
Kafka
  ^
  |
  | consume
  |
n8n Kafka Trigger
```

Consumer Group 예:

```text
n8n-pr-review
```

새 record가 발생하면 n8n workflow를 시작한다.

---

## 6. PR / Commit / Study Unit 정의

```text
PR
= 완성된 기능 전체

Commit
= 실제 개발 변경 기록

Study Unit
= 함께 리뷰/학습할 논리적 변경 묶음
```

중요:

- `commit 1개 = Gemini 1회`로 만들지 않는다.
- Study Unit이 여러 개여도 **PR당 Gemini 호출은 기본 1회**다.
- 커밋 개수와 Gemini 호출 횟수를 연결하지 않는다.

---

## 7. Study Unit 정의 방법

개발 중 commit message에 Study Unit 태그를 붙인다.

예:

```text
[study:kafka-producer] feat: GithubPrEvent DTO 구현
[study:kafka-producer] feat: Kafka producer 구현
[study:kafka-producer] test: Kafka producer 테스트 추가

[study:webhook-security] feat: GitHub signature 검증
[study:webhook-security] test: invalid signature 테스트 추가

[study:kafka-consumer] feat: n8n Kafka consumer 연동
```

n8n은 PR의 commit 목록을 가져온 뒤 그룹화한다.

```text
study:kafka-producer
  - commit A
  - commit B
  - commit C

study:webhook-security
  - commit D
  - commit E

study:kafka-consumer
  - commit F
```

Study Unit은 파일 단위가 아니라 **하나의 기술적 책임 또는 학습 주제**여야 한다.

좋은 예:

```text
GitHub Webhook을 안전하게 수신한다
GitHub 이벤트를 Kafka에 publish한다
Kafka 이벤트를 n8n이 consume한다
중복 PR 이벤트를 안전하게 처리한다
Gemini 실패 시 Ollama로 fallback한다
```

---

## 8. PR 처리 흐름

```text
Kafka Trigger
    |
    v
PR metadata 추출
    |
    v
GitHub API - PR commits 조회
    |
    v
[study:*] tag parsing
    |
    v
Study Unit grouping
    |
    v
각 Study Unit 관련 diff 구성
    |
    v
Project Context 로딩
    |
    v
Gemini Prompt 생성
    |
    v
Gemini API 1회 호출
    |
    v
JSON Parse / Validation
    |
    +--> GitHub PR Comment
    |
    +--> Slack Study Guide
```

---

## 9. Gemini 호출 정책

기본 원칙:

```text
1 PR = Gemini API 1 request
```

Study Unit이 3개든 10개든 가능한 한 하나의 request에 모두 포함한다.

Gemini 입력:

```text
PR metadata
+
PR 전체 변경 요약에 필요한 diff
+
Study Unit별 diff
+
Review Policy
+
Study Policy
+
Study Catalog
+
Project Context
```

Gemini 출력:

- PR 전체 변경 요약
- PR 전체 코드리뷰
- Study Unit별 변경 설명
- Study Unit별 리뷰 포인트
- Study Unit별 관련 학습 항목
- GitHub PR Comment용 Markdown
- Slack Study Guide용 Markdown

---

## 10. Study Guide 정책

기존의 `최대 3개` 제한은 사용하지 않는다.

목표는 **실제 코드 변경과 직접 연결되는 학습 항목을 가능한 한 빠짐없이 알려주는 것**이다.

규칙:

- 실제 PR diff와 직접 관련된 학습 주제는 모두 제시한다.
- Java 기본기 관련 항목을 누락하지 않는다.
- Effective Java와 직접 관련된 Item이 있으면 모두 제시한다.
- 함수형 프로그래밍 with 자바와 직접 관련된 Chapter/Section이 있으면 모두 제시한다.
- Spring, Kafka, DB, 네트워크, 동시성, 트랜잭션, 보안 등 추가 기술 개념도 실제 diff와 관련되면 제시한다.
- 동일 코드가 여러 학습 항목과 연결될 경우 모두 제시할 수 있다.
- 억지로 학습 항목을 생성하지 않는다.
- 존재하지 않는 Effective Java Item 또는 책 Chapter를 만들어내지 않는다.
- 각 항목에 상세 강의를 작성하지 않는다.
- 각 항목에는 `왜 이 코드와 관련 있는지` 한 줄만 작성한다.

예:

```text
## Study Unit: Kafka Producer

### Java 기본기
- CompletableFuture
  - KafkaTemplate의 비동기 전송 결과 처리와 관련

### Kafka
- Producer ACK
  - Kafka publish 성공을 언제 확정할지 결정하는 부분과 관련
- Message Key / Partition
  - 동일 PR 이벤트 순서 유지와 관련
- At-least-once delivery
  - 중복 이벤트 처리와 idempotency 설계와 관련

### Effective Java
- Item XX. ...
  - 현재 변경 코드의 ... 부분과 관련
```

---

## 11. Gemini Output Format

Structured JSON 사용을 우선한다.

예시:

```json
{
  "review_markdown": "GitHub에 등록할 전체 PR 리뷰",
  "study_markdown": "Slack에 등록할 Study Guide",
  "study_units": [
    {
      "id": "kafka-producer",
      "summary": "변경사항 요약",
      "review_points": ["..."],
      "study_topics": [
        {
          "category": "Kafka",
          "topic": "Producer ACK",
          "reason": "Kafka publish 성공 판단과 관련"
        }
      ]
    }
  ]
}
```

다음 두 필드는 반드시 유지한다.

```text
review_markdown
study_markdown
```

---

## 12. Gemini Output Token 정책

출력 공간은 지나치게 작게 제한하지 않는다.

기존 `maxOutputTokens=1800`에서는 다음 문제가 발생했다.

```text
thinkingTokens ≈ 1730
visible output ≈ 66
finishReason = MAX_TOKENS
```

초기 PoC 권장값:

```text
maxOutputTokens = 16384
```

중요:

- `16384`를 설정해도 항상 16384 token을 사용하는 것은 아니다.
- 필요한 만큼 사용하고 응답이 끝나면 종료된다.
- 출력 공간을 아끼기 위해 JSON이 중간에 잘리는 상황을 만들지 않는다.

---

## 13. Gemini 응답 검증

Gemini 응답이 이상한 상태에서 GitHub/Slack까지 진행하면 안 된다.

반드시 fail-fast한다.

실패 조건 예:

```text
candidate 없음
finishReason == MAX_TOKENS
response text 없음
JSON.parse 실패
review_markdown 없음
study_markdown 없음
```

```text
Gemini
  |
  v
finishReason 확인
  |
  +-- MAX_TOKENS --> 실패
  |
  v
JSON Parse
  |
  +-- 실패 --> workflow STOP
  |
  v
Required field 검증
  |
  +-- 실패 --> workflow STOP
  |
  v
GitHub / Slack
```

깨진 raw JSON을 GitHub 댓글로 등록하지 않는다.

---

## 14. Gemini Primary / Ollama Fallback

Gemini를 기본 리뷰 모델로 사용한다.

```text
Gemini Primary
     |
     +-- 성공 ------------------> 결과 사용
     |
     +-- 사용 불가
             |
             v
        Ollama Fallback
```

Fallback 대상으로 고려할 장애:

- 지속적인 `429` rate limit / quota 관련 오류
- 반복되는 `503` service unavailable / high demand
- 네트워크 장애로 Gemini API를 일정 시간 사용할 수 없는 경우

Fallback으로 숨기면 안 되는 오류:

- 잘못된 request body
- 잘못된 JSON schema
- prompt 생성 로직 버그
- n8n Code Node 오류
- 설정 실수

이러한 오류는 workflow를 실패시켜 원인을 수정한다.

---

## 15. Kafka를 사용하는 이유

Kafka는 단순 학습용 기술이 아니다.

해결하려는 실제 문제:

```text
GitHub PR 발생
    |
    v
집 n8n 서버 OFF
    |
    v
Webhook 유실
    |
    v
PR Review 미실행
```

Kafka 적용 후:

```text
GitHub
  |
  v
OCI Spring Boot
  |
  v
Kafka
  |
  | record 유지
  |
집 n8n OFF

...

집 n8n ON
  |
  v
마지막 consumer offset 이후 record consume
  |
  v
밀린 PR Review 수행
```

Kafka는 **OCI가 이미 수신한 이벤트를 n8n 장애와 분리하여 보존**하기 위해 사용한다.

---

## 16. Kafka가 해결하지 못하는 장애

OCI 자체가 죽어 GitHub Webhook을 받지 못하면 Kafka에도 이벤트가 들어오지 않는다.

따라서 후속 단계에서 Reconciliation Workflow를 고려한다.

```text
Schedule
  |
  v
GitHub Open PR 조회
  |
  v
현재 headSha
  |
  v
lastReviewedHeadSha 비교
  |
  +-- 동일 --> skip
  |
  +-- 다름 --> review
```

역할:

```text
Kafka
= 받은 이벤트를 잃지 않는다.

Reconciliation
= 아예 받지 못한 이벤트도 찾아낸다.
```

PoC 1차 범위에서는 Kafka 기반 delivery를 먼저 완성한다.

---

## 17. 중복 처리 / Idempotency

동일 이벤트가 여러 번 처리될 수 있다고 가정한다.

최소 식별 기준:

```text
repo
prNumber
headSha
```

개념:

```text
incoming headSha == lastReviewedHeadSha
    |
    +--> 이미 리뷰한 상태이므로 skip

incoming headSha != lastReviewedHeadSha
    |
    +--> 새 변경이므로 review
```

`deliveryId`도 webhook 단위 추적에 사용한다.

---

## 18. 네트워크

집 n8n은 Kafka Consumer이므로 OCI가 n8n의 public URL을 알 필요가 없다.

```text
Home n8n
    |
    | outbound connection
    v
OCI Kafka
```

Kafka를 public internet에 그대로 노출하지 않는 것을 우선한다.

OCI와 Home Server 사이 private network 후보:

- Tailscale
- WireGuard

Cloudflare Tunnel은 n8n UI 외부 접근용으로 유지할 수 있으나 Kafka consume을 위해 필요한 것은 아니다.

---

## 19. OCI PoC 구성

현재 목표에서는 PostgreSQL이 필수는 아니다.

```text
OCI 2 CPU / 12GB

Docker Compose
|
+-- Spring Boot Webhook Receiver
|
+-- Kafka
    +-- KRaft single broker
```

PoC에서 불필요:

```text
OCI n8n
PostgreSQL
Ollama
```

Home Server:

```text
n8n
Ollama
```

---

## 20. 개발 순서

Claude Code는 한 번에 모든 기능을 구현하지 말고 아래 Phase 순서대로 작업한다.

각 단계는 독립적으로 실행/테스트 가능한 상태에서 완료한다.

### Phase 1 - Kafka 인프라

- Kafka KRaft single broker 구성
- `github.pr.events` topic 준비
- local producer/consumer 테스트
- Docker Compose 구성

완료 조건:

```text
producer가 넣은 테스트 record를 consumer가 정상적으로 읽는다.
```

### Phase 2 - GitHub Webhook Receiver

- Spring Boot endpoint 생성
- GitHub webhook payload 수신
- event/action parsing
- `X-Hub-Signature-256` 검증
- 필요한 metadata 추출

완료 조건:

```text
GitHub test delivery 또는 테스트 payload를 안전하게 수신한다.
```

### Phase 3 - Kafka Producer

- GithubPrEvent 정의
- Kafka producer 구현
- 4장 Produce 계약(`deliveryId/event/action/repo/prNumber/headSha`) 형태로 publish
- Kafka key `{repo}:{prNumber}` 사용
- broker ACK 확인 후 HTTP 성공 응답

완료 조건:

```text
실제 GitHub webhook이 Kafka record로 저장된다.
```

### Phase 4 - n8n Kafka Consumer

- Kafka Trigger 구성
- Consumer Group 지정
- Kafka event parsing
- 기존 PR review workflow와 연결

완료 조건:

```text
Kafka record 발생 시 n8n workflow가 실행된다.
```

### Phase 5 - Study Unit Grouping

- GitHub API에서 PR commit 목록 조회
- `[study:*]` tag parsing
- 같은 Study Unit commit grouping
- 각 Study Unit에 필요한 diff 수집
- Study Unit 전체를 하나의 Gemini prompt에 포함

완료 조건:

```text
commit 수와 관계없이 PR당 Gemini request는 기본 1회다.
```

### Phase 6 - Gemini Review + Study Guide

- PR 전체 review 생성
- Study Unit별 study guide 생성
- 학습 항목 개수 제한 없음
- 실제 변경과 관련된 학습 항목 모두 제시
- Structured JSON 응답
- fail-fast parse 적용

완료 조건:

```text
GitHub에는 review_markdown,
Slack에는 study_markdown이 정상적으로 전달된다.
```

### Phase 7 - Gemini Fallback

- retry/backoff
- 429/503 분류
- Gemini 지속 실패 시 Ollama fallback
- 애플리케이션 버그는 fallback하지 않고 실패 처리

완료 조건:

```text
Gemini 일시 장애 시 Ollama로 리뷰가 완료된다.
```

### Phase 8 - Reliability

후속 작업:

- duplicate 처리
- lastReviewedHeadSha
- reconciliation
- monitoring/logging
- dead-letter 전략 검토

---

## 21. Claude Code가 지켜야 할 개발 원칙

1. 과도한 추상화를 먼저 만들지 않는다.
2. PoC에서 실제로 필요한 최소 기능부터 구현한다.
3. DB가 필요하지 않은 단계에서 PostgreSQL을 추가하지 않는다.
4. Kafka message에는 거대한 PR diff를 넣지 않는다.
5. GitHub API에서 최신 상태를 조회한다.
6. commit 개수만큼 Gemini API를 호출하지 않는다.
7. 기본 정책은 `1 PR = 1 Gemini request`다.
8. Study Unit은 학습/리뷰를 위한 grouping metadata다.
9. 학습 항목은 최대 개수로 자르지 않는다.
10. 실제 변경과 직접 관련된 항목만 제시한다.
11. Gemini가 실패했다고 모든 오류를 Ollama로 숨기지 않는다.
12. 잘못된 JSON/설정/코드 오류는 fail-fast한다.
13. secret/token/webhook URL을 코드와 로그에 노출하지 않는다.
14. 동일 PR 이벤트가 재전달될 수 있다고 가정한다.
15. 각 단계가 완료되면 테스트 결과와 남은 위험요소를 보고한다.
16. 사용자가 요청하지 않은 다음 Phase를 임의로 구현하지 않는다.

---

## 22. Claude Code 작업 방식

새 작업을 시작할 때 이 문서를 기준으로 현재 Phase를 확인한다.

사용자가 특정 Phase를 요청하면:

1. 현재 코드 구조를 먼저 확인한다.
2. 해당 Phase에 필요한 최소 변경 범위를 정리한다.
3. 다른 Phase의 기능을 선행 구현하지 않는다.
4. 변경 후 빌드/테스트를 수행한다.
5. 변경 파일과 이유를 요약한다.
6. 아직 구현하지 않은 후속 Phase를 명확하게 구분한다.

---

## 23. 최종 개발/학습 루프

```text
개발
  |
  v
의미 있는 commit + [study:*] tag
  |
  v
기능 전체 완성
  |
  v
PR 생성
  |
  v
GitHub Webhook
  |
  v
Spring Boot
  |
  v
Kafka
  |
  v
n8n
  |
  v
Study Unit grouping
  |
  v
Gemini 1회
  |
  +--> PR 전체 코드리뷰
  |
  +--> Study Unit별 학습 가이드
  |
  v
GitHub + Slack
  |
  v
개발자가 실제 코드와 연결해 학습
```

> **핵심 원칙: PR은 완성된 기능 단위로 유지하고, commit을 Study Unit으로 그룹화하여 하나의 Gemini 호출 안에서 전체 코드리뷰와 세분화된 학습 가이드를 생성한다.**

### 이 레포 자체의 적용 방식

이 파이프라인을 만드는 과정도 같은 루프로 리뷰받는다.
파이프라인이 동작하기 전에는 리뷰할 수 없으므로, 개발 브랜치 하나에서 완성까지 커밋을 쌓고
완성 후 기능 단위 PR(`pr/01-kafka-foundation`, `pr/02-webhook-receiver`, `pr/03-kafka-producer` ...)로 나눠 순서대로 연다.
PR 하나 = Gemini 호출 1회 = 그 기능의 Study Unit별 리뷰/학습 가이드. 운영 규칙은 README "브랜치 / PR 운영".

---

## 24. 기술 스택 및 레포 구조 (Spring Webhook Receiver)

레포: `github.com/owencity/n8n_kafka_api`

```text
Java 21
Spring Boot 4.1 (webmvc, kafka, actuator, validation)
Gradle Kotlin DSL
Docker (멀티스테이지) + Docker Compose
GitHub Actions: build/test → GHCR publish (amd64/arm64) → OCI SSH 배포
Kafka: apache/kafka 4.3.x (KRaft single broker)
```

구조:

- 단일 모듈 모놀리스
- 패키지: `com.owencity.n8nkafka` 아래 기능 단위
  - `webhook`: Webhook 수신, 서명 검증, payload 파싱 (Phase 2)
  - `event`: `GithubPrEvent`, Kafka producer (Phase 3)

Spring AI는 도입하지 않는다. LLM 호출은 n8n이 담당하며, n8n과 AI API 사이에 Spring 계층을 추가하는 것은 이점 없이 홉만 늘린다.
n8n이 한계에 부딪히는 경우(실패한 실행의 offset이 commit되어 메시지가 유실되거나, Code Node 로직을 테스트 없이 관리하기 어려워지는 경우)에만 consumer 자리를 Spring Kafka로 교체하는 것을 검토한다.

Phase 2~3 결정 사항:

- Producer: `acks=all`, `enable.idempotence=true`, 전송 타임아웃은 GitHub webhook 타임아웃(10초)보다 짧게
- 응답 코드: 대상 PR 이벤트 202, `ping` 200, 대상 외 event/action 204, 서명 오류 401, 잘못된 payload 400, Kafka publish 실패 5xx
- GitHub webhook Content type은 `application/json` (서명은 body 원문 바이트 기준)
- Kafka 실패로 5xx를 반환해도 GitHub는 자동 재전송하지 않는다. 이 빈틈은 16장 Reconciliation이 메운다.
