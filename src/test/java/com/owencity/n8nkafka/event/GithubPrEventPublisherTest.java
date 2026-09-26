package com.owencity.n8nkafka.event;

import org.apache.kafka.common.errors.TimeoutException;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * broker ACK를 받지 못한 모든 경우가 {@link EventPublishException}으로 드러나는지 확인한다.
 * 컨트롤러는 이 예외를 받아야만 GitHub에 실패 응답을 보낼 수 있다.
 */
class GithubPrEventPublisherTest {

    private static final GithubPrEvent EVENT = new GithubPrEvent(
            "delivery-1", "pull_request", "opened", "owencity/n8n_kafka", 4, "abc123");

    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);

    private final GithubPrEventPublisher publisher = new GithubPrEventPublisher(
            kafkaTemplate,
            JsonMapper.builder().build(),
            new GithubPrEventKafkaProperties("github.pr.events", Duration.ofMillis(200))
    );

    @Test
    void failsWhenBrokerRejectsRecord() {
        when(kafkaTemplate.send(eq("github.pr.events"), eq("owencity/n8n_kafka:4"), anyString()))
                .thenReturn(CompletableFuture.failedFuture(new TimeoutException("delivery timeout")));

        assertThatThrownBy(() -> publisher.publish(EVENT))
                .isInstanceOf(EventPublishException.class)
                .hasMessageContaining("delivery-1")
                .hasCauseInstanceOf(TimeoutException.class);
    }

    @Test
    void failsWhenSendThrowsImmediately() {
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenThrow(new KafkaException("metadata not available"));

        assertThatThrownBy(() -> publisher.publish(EVENT))
                .isInstanceOf(EventPublishException.class)
                .hasCauseInstanceOf(KafkaException.class);
    }

    @Test
    void failsWhenAckDoesNotArriveInTime() {
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenReturn(new CompletableFuture<SendResult<String, String>>());

        assertThatThrownBy(() -> publisher.publish(EVENT))
                .isInstanceOf(EventPublishException.class)
                .hasCauseInstanceOf(java.util.concurrent.TimeoutException.class);
    }
}
