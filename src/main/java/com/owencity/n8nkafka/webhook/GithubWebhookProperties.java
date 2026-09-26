package com.owencity.n8nkafka.webhook;

import jakarta.validation.constraints.NotBlank;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * GitHub webhook 설정. secret이 비어 있으면 애플리케이션이 기동되지 않는다.
 */
@Validated
@ConfigurationProperties("github.webhook")
public record GithubWebhookProperties(@NotBlank String secret) {

    // record 기본 toString은 모든 필드를 출력하므로, 로그에 secret이 남지 않게 가린다.
    @Override
    public String toString() {
        return "GithubWebhookProperties[secret=****]";
    }
}
