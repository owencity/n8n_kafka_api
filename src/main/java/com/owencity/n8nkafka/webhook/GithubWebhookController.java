package com.owencity.n8nkafka.webhook;

import com.owencity.n8nkafka.event.GithubPrEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.util.Set;

/**
 * GitHub webhook 수신 엔드포인트. GitHub webhook 설정의 Content type은 {@code application/json}이어야 한다.
 *
 * <ul>
 *   <li>401: 서명 누락/불일치</li>
 *   <li>200: {@code ping}</li>
 *   <li>204: 처리 대상이 아닌 event/action</li>
 *   <li>400: 서명은 유효하지만 payload/헤더가 잘못됨</li>
 *   <li>202: 처리 대상 PR 이벤트 수신</li>
 * </ul>
 */
@RestController
@RequestMapping("/webhooks/github")
public class GithubWebhookController {

    private static final Logger log = LoggerFactory.getLogger(GithubWebhookController.class);

    private static final Set<String> TARGET_ACTIONS = Set.of("opened", "synchronize", "reopened");

    private final GithubSignatureVerifier signatureVerifier;
    private final JsonMapper jsonMapper;

    public GithubWebhookController(GithubSignatureVerifier signatureVerifier, JsonMapper jsonMapper) {
        this.signatureVerifier = signatureVerifier;
        this.jsonMapper = jsonMapper;
    }

    // 서명은 body 원문 바이트로 계산해야 하므로 객체가 아닌 byte[]로 받는다.
    @PostMapping
    public ResponseEntity<Void> receive(
            @RequestHeader(name = "X-GitHub-Event", required = false) String event,
            @RequestHeader(name = "X-GitHub-Delivery", required = false) String deliveryId,
            @RequestHeader(name = "X-Hub-Signature-256", required = false) String signature,
            @RequestBody(required = false) byte[] body
    ) {
        byte[] payloadBytes = body == null ? new byte[0] : body;
        if (!signatureVerifier.isValid(payloadBytes, signature)) {
            log.warn("서명 검증 실패: deliveryId={}, event={}", deliveryId, event);
            return ResponseEntity.status(401).build();
        }

        if ("ping".equals(event)) {
            log.info("ping 수신: deliveryId={}", deliveryId);
            return ResponseEntity.ok().build();
        }
        if (!PullRequestPayloads.EVENT.equals(event)) {
            return ResponseEntity.noContent().build();
        }

        JsonNode payload = jsonMapper.readTree(payloadBytes);
        String action = PullRequestPayloads.action(payload);
        if (!TARGET_ACTIONS.contains(action)) {
            return ResponseEntity.noContent().build();
        }
        if (deliveryId == null || deliveryId.isBlank()) {
            log.warn("X-GitHub-Delivery 헤더 누락: action={}", action);
            return ResponseEntity.badRequest().build();
        }

        GithubPrEvent prEvent = PullRequestPayloads.toEvent(deliveryId, payload);
        log.info("PR 이벤트 수신: deliveryId={}, repo={}, prNumber={}, action={}, headSha={}",
                prEvent.deliveryId(), prEvent.repo(), prEvent.prNumber(), prEvent.action(), prEvent.headSha());
        return ResponseEntity.accepted().build();
    }

    @ExceptionHandler({JacksonException.class, InvalidWebhookPayloadException.class})
    ResponseEntity<Void> handleInvalidPayload(RuntimeException e) {
        log.warn("잘못된 webhook payload: {}", e.getMessage());
        return ResponseEntity.badRequest().build();
    }
}
