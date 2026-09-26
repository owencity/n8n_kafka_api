package com.owencity.n8nkafka.event;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.util.Objects;

/**
 * {@code github.pr.events} topic에 produce하는 메시지 계약.
 *
 * <p>n8n consumer가 이 JSON 형태에 의존한다. 필드 이름, 타입, 개수를 바꾸면 consumer가 깨지므로
 * {@code GithubPrEventContractTest}와 {@code contract/github-pr-event.json}을 함께 갱신하고 n8n 쪽과 합의해야 한다.
 *
 * <p>{@code headRef}/{@code baseRef}는 n8n 리뷰 프롬프트가 사용하는 브랜치 정보로, 필드 추가만 했으므로 기존 필드를
 * 읽는 consumer에는 영향이 없다. 값은 이벤트 발생 시점 기준이다.
 */
@JsonPropertyOrder({"deliveryId", "event", "action", "repo", "prNumber", "headSha", "headRef", "baseRef"})
public record GithubPrEvent(
        String deliveryId,
        String event,
        String action,
        String repo,
        int prNumber,
        String headSha,
        String headRef,
        String baseRef
) {

    public GithubPrEvent {
        requireText(deliveryId, "deliveryId");
        requireText(event, "event");
        requireText(action, "action");
        requireText(repo, "repo");
        requireText(headSha, "headSha");
        requireText(headRef, "headRef");
        requireText(baseRef, "baseRef");
        if (prNumber <= 0) {
            throw new IllegalArgumentException("prNumber must be positive: " + prNumber);
        }
    }

    /** 같은 PR의 이벤트가 같은 partition으로 가도록 하는 Kafka record key. */
    public String kafkaKey() {
        return repo + ":" + prNumber;
    }

    private static void requireText(String value, String name) {
        Objects.requireNonNull(value, name + " must not be null");
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
    }
}
