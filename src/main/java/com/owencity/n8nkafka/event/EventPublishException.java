package com.owencity.n8nkafka.event;

/**
 * Kafka broker로부터 ACK를 받지 못해 이벤트 저장을 확정할 수 없을 때 던진다.
 */
public class EventPublishException extends RuntimeException {

    public EventPublishException(String message, Throwable cause) {
        super(message, cause);
    }
}
