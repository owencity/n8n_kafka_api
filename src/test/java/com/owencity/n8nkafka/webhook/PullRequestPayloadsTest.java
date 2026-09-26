package com.owencity.n8nkafka.webhook;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.io.InputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PullRequestPayloadsTest {

    private static final String DELIVERY_ID = "72d3162e-cc78-11e3-81ab-4c9367dc0958";

    private final JsonMapper mapper = JsonMapper.builder().build();

    @Test
    void convertsGithubPayloadToProduceContract() throws IOException {
        JsonNode payload = read("/webhook/pull_request.synchronize.json");

        JsonNode produced = mapper.valueToTree(PullRequestPayloads.toEvent(DELIVERY_ID, payload));

        // GitHub webhook fixture가 계약 fixture로 정확히 변환되는지 확인한다.
        assertThat(produced).isEqualTo(read("/contract/github-pr-event.json"));
    }

    @Test
    void rejectsPayloadWithoutHeadSha() throws IOException {
        ObjectNode payload = (ObjectNode) read("/webhook/pull_request.synchronize.json");
        ((ObjectNode) payload.get("pull_request").get("head")).remove("sha");

        assertThatThrownBy(() -> PullRequestPayloads.toEvent(DELIVERY_ID, payload))
                .isInstanceOf(InvalidWebhookPayloadException.class)
                .hasMessageContaining("headSha");
    }

    @Test
    void rejectsPayloadWithoutBaseRef() throws IOException {
        ObjectNode payload = (ObjectNode) read("/webhook/pull_request.synchronize.json");
        ((ObjectNode) payload.get("pull_request").get("base")).remove("ref");

        assertThatThrownBy(() -> PullRequestPayloads.toEvent(DELIVERY_ID, payload))
                .isInstanceOf(InvalidWebhookPayloadException.class)
                .hasMessageContaining("baseRef");
    }

    @Test
    void rejectsPayloadWithoutRepository() throws IOException {
        ObjectNode payload = (ObjectNode) read("/webhook/pull_request.synchronize.json");
        payload.remove("repository");

        assertThatThrownBy(() -> PullRequestPayloads.toEvent(DELIVERY_ID, payload))
                .isInstanceOf(InvalidWebhookPayloadException.class)
                .hasMessageContaining("repo");
    }

    private JsonNode read(String path) throws IOException {
        try (InputStream in = getClass().getResourceAsStream(path)) {
            return mapper.readTree(in);
        }
    }
}
