package com.owencity.n8nkafka.event;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

/**
 * PR 이벤트 publish 설정.
 *
 * @param topic       produce 대상 topic
 * @param sendTimeout broker ACK를 기다리는 최대 시간. GitHub webhook 타임아웃(10초)보다 짧아야 한다.
 */
@Validated
@ConfigurationProperties("github.pr-events")
public record GithubPrEventKafkaProperties(
        @NotBlank String topic,
        @NotNull Duration sendTimeout
) {
}
