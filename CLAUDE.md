# CLAUDE.md

이 레포의 설계 기준은 [docs/N8N_KAFKA_PR_REVIEW_STUDY_GUIDE_SPEC.md](docs/N8N_KAFKA_PR_REVIEW_STUDY_GUIDE_SPEC.md)다.
작업 전 명세의 20~22장(개발 순서, 개발 원칙, 작업 방식)을 확인하고, 요청받은 Phase만 구현한다.

- 이 레포는 Webhook 수신 → 검증 → Kafka publish까지만 담당한다. AI 호출, GitHub API 조회는 n8n 몫이다.
- 빌드/테스트: `./gradlew build`
- 패키지: `com.owencity.n8nkafka` 아래 기능 단위(`webhook`, `event`). 필요할 때 만든다.
- 브랜치 하나에서 개발하고 PR은 기능 완성 후 한 번 만든다. 커밋에는 `[study:<unit>]` 태그를 붙이고, 구현과 테스트를 나눠 커밋한다.
- secret은 환경변수로만 받고 로그에 출력하지 않는다.
