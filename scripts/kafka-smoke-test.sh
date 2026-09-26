#!/usr/bin/env bash
# Phase 1 완료 조건 확인 (docker compose up -d 이후 실행)
#   1. producer가 넣은 record를 consumer가 읽는다.
#   2. broker 컨테이너가 새로 만들어져도 record와 consumer offset이 유지되어, 같은 consumer group은
#      이미 읽은 record는 건너뛰고 밀린 record부터 읽는다. (n8n이 꺼졌다 켜지는 상황)
set -euo pipefail
cd "$(dirname "$0")/.."
# Windows Git Bash가 /opt/... 경로를 C:/Program Files/Git/opt/... 로 바꾸지 않게 한다.
export MSYS_NO_PATHCONV=1

TOPIC=github.pr.events
BOOTSTRAP=kafka:19092
RUN="smoke-$(date +%s)"
GROUP="$RUN-group"
KEY="owencity/n8n_kafka:4"

kafka_cli() {
  local tool=$1; shift
  docker compose exec -T kafka "/opt/kafka/bin/$tool" "$@"
}

produce() {
  local delivery_id=$1
  local json
  json=$(printf '{"deliveryId":"%s","event":"pull_request","action":"synchronize","repo":"owencity/n8n_kafka","prNumber":4,"headSha":"6dcb09b5b57875f334f61aebed695e2e4193db5e"}' "$delivery_id")
  printf '%s|%s\n' "$KEY" "$json" | kafka_cli kafka-console-producer.sh \
    --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC" \
    --reader-property parse.key=true --reader-property key.separator='|'
}

consume() {
  kafka_cli kafka-console-consumer.sh \
    --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC" --group "$GROUP" "$@" \
    --formatter-property print.key=true --formatter-property key.separator=' => ' \
    --timeout-ms 10000 2>/dev/null || true
}

wait_healthy() {
  for _ in $(seq 1 30); do
    [ "$(docker compose ps kafka --format '{{.Health}}')" = "healthy" ] && return 0
    sleep 2
  done
  echo "kafka가 healthy 상태가 되지 않았다" >&2
  exit 1
}

fail() { echo "FAIL: $*" >&2; exit 1; }

echo "== 1. produce → consume"
produce "$RUN-1"
out=$(consume --from-beginning)
echo "$out" | grep "$RUN"
echo "$out" | grep -q "^$KEY => .*\"$RUN-1\"" || fail "$RUN-1 을 읽지 못했다"

echo "== 2. consumer가 꺼진 동안 새 이벤트 도착 + broker 컨테이너 재생성 (데이터는 볼륨에 유지)"
produce "$RUN-2"
docker compose up -d --force-recreate --no-deps kafka > /dev/null 2>&1
wait_healthy

echo "== 3. 같은 consumer group으로 재개"
out=$(consume)
echo "$out" | grep "$RUN" || true
echo "$out" | grep -q "\"$RUN-2\"" || fail "재시작 후 밀린 record($RUN-2)를 읽지 못했다"
if echo "$out" | grep -q "\"$RUN-1\""; then fail "이미 읽은 record($RUN-1)를 다시 읽었다"; fi

echo "PASS"
