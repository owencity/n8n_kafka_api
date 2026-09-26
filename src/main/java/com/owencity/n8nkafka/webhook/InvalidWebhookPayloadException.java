package com.owencity.n8nkafka.webhook;

/**
 * 서명은 유효하지만 payload에서 produce 계약에 필요한 값을 얻을 수 없을 때 던진다.
 */
class InvalidWebhookPayloadException extends RuntimeException {

    InvalidWebhookPayloadException(String message, Throwable cause) {
        super(message, cause);
    }
}
