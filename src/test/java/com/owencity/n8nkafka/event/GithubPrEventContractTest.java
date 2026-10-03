package com.owencity.n8nkafka.event;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * n8n consumer와 맺은 produce 계약을 고정한다.
 * 이 테스트가 깨졌다면 코드를 고치기 전에 계약 변경이 의도된 것인지 먼저 확인한다.
 */
class GithubPrEventContractTest {

    private final JsonMapper mapper = JsonMapper.builder().build();

    private final GithubPrEvent sample = new GithubPrEvent(
            "72d3162e-cc78-11e3-81ab-4c9367dc0958",
            "pull_request",
            "synchronize",
            "owencity/n8n_kafka",
            4,
            "6dcb09b5b57875f334f61aebed695e2e4193db5e"
    );

    @Test
    void serializesExactlyToContractFixture() throws IOException {
        JsonNode actual = mapper.readTree(mapper.writeValueAsString(sample));

        assertThat(actual).isEqualTo(readFixture());
    }

    @Test
    void hasOnlyContractFieldsInOrder() {
        JsonNode actual = mapper.valueToTree(sample);

        assertThat(actual.propertyNames())
                .containsExactly("deliveryId", "event", "action", "repo", "prNumber", "headSha");
    }

    @Test
    void prNumberIsJsonNumber() {
        JsonNode actual = mapper.valueToTree(sample);

        assertThat(actual.get("prNumber").isInt()).isTrue();
    }

    @Test
    void deserializesFromContractFixture() throws IOException {
        GithubPrEvent parsed = mapper.treeToValue(readFixture(), GithubPrEvent.class);

        assertThat(parsed).isEqualTo(sample);
    }

    @Test
    void kafkaKeyIsRepoAndPrNumber() {
        assertThat(sample.kafkaKey()).isEqualTo("owencity/n8n_kafka:4");
    }

    @Test
    void rejectsMissingOrInvalidFields() {
        for (Runnable invalid : List.<Runnable>of(
                () -> new GithubPrEvent(null, "pull_request", "opened", "o/r", 1, "sha"),
                () -> new GithubPrEvent("id", " ", "opened", "o/r", 1, "sha"),
                () -> new GithubPrEvent("id", "pull_request", "opened", "", 1, "sha"),
                () -> new GithubPrEvent("id", "pull_request", "opened", "o/r", 0, "sha"),
                () -> new GithubPrEvent("id", "pull_request", "opened", "o/r", 1, null)
        )) {
            assertThatThrownBy(invalid::run).isInstanceOf(RuntimeException.class);
        }
    }

    private JsonNode readFixture() throws IOException {
        try (InputStream in = getClass().getResourceAsStream("/contract/github-pr-event.json")) {
            return mapper.readTree(in);
        }
    }
}
