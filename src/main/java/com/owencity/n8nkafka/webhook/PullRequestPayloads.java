package com.owencity.n8nkafka.webhook;

import com.owencity.n8nkafka.event.GithubPrEvent;
import tools.jackson.databind.JsonNode;

/**
 * GitHub {@code pull_request} webhook payload를 produce 계약({@link GithubPrEvent})으로 변환한다.
 */
final class PullRequestPayloads {

    static final String EVENT = "pull_request";

    private PullRequestPayloads() {
    }

    static String action(JsonNode payload) {
        return payload.path("action").asString();
    }

    static GithubPrEvent toEvent(String deliveryId, JsonNode payload) {
        JsonNode pullRequest = payload.path("pull_request");
        try {
            return new GithubPrEvent(
                    deliveryId,
                    EVENT,
                    action(payload),
                    payload.path("repository").path("full_name").asString(),
                    pullRequest.path("number").asInt(),
                    pullRequest.path("head").path("sha").asString(),
                    pullRequest.path("head").path("ref").asString(),
                    pullRequest.path("base").path("ref").asString()
            );
        } catch (IllegalArgumentException e) {
            throw new InvalidWebhookPayloadException("pull_request payload에 필수 값이 없다: " + e.getMessage(), e);
        }
    }
}
