#!/usr/bin/env bash
# GitHub와 같은 형식(헤더 + HMAC-SHA256 서명)으로 pull_request webhook을 보낸다.
#
# 사용: bash scripts/send-test-webhook.sh [action] [url]
#   action 기본값: synchronize (opened, reopened, closed 등)
#   url    기본값: http://localhost:8080/webhooks/github
# secret은 GITHUB_WEBHOOK_SECRET 환경변수 또는 .env에서 읽는다.
set -euo pipefail
cd "$(dirname "$0")/.."

if [ -z "${GITHUB_WEBHOOK_SECRET:-}" ] && [ -f .env ]; then
  set -a
  # shellcheck disable=SC1091
  . ./.env
  set +a
fi
: "${GITHUB_WEBHOOK_SECRET:?GITHUB_WEBHOOK_SECRET 환경변수 또는 .env가 필요하다}"

ACTION=${1:-synchronize}
URL=${2:-http://localhost:8080/webhooks/github}
DELIVERY_ID="local-$(date +%s)-$RANDOM"

# 서명은 바이트 단위로 일치해야 한다. body를 셸 변수/명령 인자로 넘기면 OS에 따라 인코딩이 바뀔 수 있으므로
# (Windows에서 한글이 깨짐) 파일 하나로 서명과 전송을 모두 한다.
body_file=$(mktemp)
trap 'rm -f "$body_file"' EXIT
sed "s/\"action\": \"synchronize\"/\"action\": \"$ACTION\"/" src/test/resources/webhook/pull_request.synchronize.json > "$body_file"
signature="sha256=$(openssl dgst -sha256 -hmac "$GITHUB_WEBHOOK_SECRET" < "$body_file" | awk '{print $NF}')"

echo "POST $URL (action=$ACTION, deliveryId=$DELIVERY_ID)"
curl -sS -o /dev/null -w "HTTP %{http_code}\n" -X POST "$URL" \
  -H "Content-Type: application/json" \
  -H "X-GitHub-Event: pull_request" \
  -H "X-GitHub-Delivery: $DELIVERY_ID" \
  -H "X-Hub-Signature-256: $signature" \
  --data-binary "@$body_file"
