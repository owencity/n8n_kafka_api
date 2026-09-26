package com.owencity.n8nkafka.event;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 실제 broker(Embedded Kafka)에 저장된 record가 produce 계약과 같은지 확인한다.
 * n8n consumer가 받게 될 바이트를 직접 검증하는 테스트다.
 */
@SpringBootTest(properties = "github.webhook.secret=test-secret")
@EmbeddedKafka(topics = "github.pr.events", partitions = 3, bootstrapServersProperty = "spring.kafka.bootstrap-servers")
class GithubPrEventPublisherKafkaTest {

    private static final String TOPIC = "github.pr.events";

    @Autowired
    private GithubPrEventPublisher publisher;

    @Autowired
    private EmbeddedKafkaBroker broker;

    private final JsonMapper mapper = JsonMapper.builder().build();

    @Test
    void storesRecordExactlyAsProduceContract() throws IOException {
        GithubPrEvent event = new GithubPrEvent(
                "72d3162e-cc78-11e3-81ab-4c9367dc0958",
                "pull_request",
                "synchronize",
                "owencity/n8n_kafka",
                4,
                "6dcb09b5b57875f334f61aebed695e2e4193db5e",
                "feat/webhook-kafka-pipeline",
                "main"
        );

        publisher.publish(event);

        try (KafkaConsumer<String, String> consumer = newConsumer()) {
            consumer.subscribe(List.of(TOPIC));
            ConsumerRecord<String, String> record = KafkaTestUtils.getSingleRecord(consumer, TOPIC, Duration.ofSeconds(10));

            assertThat(record.key()).isEqualTo("owencity/n8n_kafka:4");
            assertThat(mapper.readTree(record.value())).isEqualTo(readContractFixture());
            assertThat(record.headers().lastHeader("__TypeId__")).isNull();
        }
    }

    private KafkaConsumer<String, String> newConsumer() {
        return new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString(),
                ConsumerConfig.GROUP_ID_CONFIG, "contract-test",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class
        ));
    }

    private JsonNode readContractFixture() throws IOException {
        try (InputStream in = getClass().getResourceAsStream("/contract/github-pr-event.json")) {
            return mapper.readTree(in);
        }
    }
}
