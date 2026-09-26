package com.owencity.n8nkafka.event;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * {@link GithubPrEvent}를 Kafka에 publish하고 broker ACK까지 기다린다.
 *
 * <p>value는 직접 JSON 문자열로 직렬화해 보낸다. Spring Kafka의 JSON serializer가 붙이는
 * {@code __TypeId__} 헤더 없이 produce 계약 JSON만 싣기 위해서다.
 */
@Component
public class GithubPrEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(GithubPrEventPublisher.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final JsonMapper jsonMapper;
    private final GithubPrEventKafkaProperties properties;

    public GithubPrEventPublisher(
            KafkaTemplate<String, String> kafkaTemplate,
            JsonMapper jsonMapper,
            GithubPrEventKafkaProperties properties
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.jsonMapper = jsonMapper;
        this.properties = properties;
    }

    /**
     * broker가 record 저장을 확인(ACK)한 뒤에 반환한다.
     *
     * @throws EventPublishException ACK를 받지 못한 경우. 호출자는 GitHub에 성공 응답을 보내면 안 된다.
     */
    public void publish(GithubPrEvent event) {
        String value = jsonMapper.writeValueAsString(event);
        try {
            RecordMetadata metadata = kafkaTemplate.send(properties.topic(), event.kafkaKey(), value)
                    .get(properties.sendTimeout().toMillis(), TimeUnit.MILLISECONDS)
                    .getRecordMetadata();
            log.info("PR 이벤트 publish 완료: deliveryId={}, key={}, {}-{}@{}",
                    event.deliveryId(), event.kafkaKey(), metadata.topic(), metadata.partition(), metadata.offset());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw failure(event, e);
        } catch (ExecutionException e) {
            throw failure(event, e.getCause());
        } catch (TimeoutException | RuntimeException e) {
            // RuntimeException: metadata를 가져오지 못하는 등 send() 호출 자체가 실패한 경우
            throw failure(event, e);
        }
    }

    private EventPublishException failure(GithubPrEvent event, Throwable cause) {
        return new EventPublishException(
                "PR 이벤트 publish 실패: deliveryId=" + event.deliveryId() + ", key=" + event.kafkaKey(), cause);
    }
}
