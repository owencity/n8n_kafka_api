package com.owencity.n8nkafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.web.servlet.assertj.MockMvcTester;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.time.Duration;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 3 완료 조건: GitHub webhook 요청이 Kafka record(produce 계약)로 저장된다.
 */
@SpringBootTest(properties = "github.webhook.secret=" + WebhookToKafkaIntegrationTest.SECRET)
@AutoConfigureMockMvc
@EmbeddedKafka(topics = "github.pr.events", partitions = 3, bootstrapServersProperty = "spring.kafka.bootstrap-servers")
class WebhookToKafkaIntegrationTest {

    static final String SECRET = "integration-secret";
    private static final String TOPIC = "github.pr.events";
    private static final String DELIVERY_ID = "72d3162e-cc78-11e3-81ab-4c9367dc0958";

    @Autowired
    private MockMvcTester mvc;

    @Autowired
    private EmbeddedKafkaBroker broker;

    private final JsonMapper mapper = JsonMapper.builder().build();

    @Test
    void signedPullRequestWebhookIsStoredAsProduceContract() throws IOException {
        byte[] body = readResource("/webhook/pull_request.synchronize.json");

        assertThat(mvc.post().uri("/webhooks/github")
                .contentType(MediaType.APPLICATION_JSON)
                .header("X-GitHub-Event", "pull_request")
                .header("X-GitHub-Delivery", DELIVERY_ID)
                .header("X-Hub-Signature-256", sign(body))
                .content(body)
                .exchange()).hasStatus(202);

        try (KafkaConsumer<String, String> consumer = newConsumer()) {
            consumer.subscribe(List.of(TOPIC));
            ConsumerRecord<String, String> record = KafkaTestUtils.getSingleRecord(consumer, TOPIC, Duration.ofSeconds(10));

            assertThat(record.key()).isEqualTo("owencity/n8n_kafka:4");
            assertThat(mapper.readTree(record.value()))
                    .isEqualTo(mapper.readTree(readResource("/contract/github-pr-event.json")));
        }
    }

    private KafkaConsumer<String, String> newConsumer() {
        return new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString(),
                ConsumerConfig.GROUP_ID_CONFIG, "webhook-integration-test",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class
        ));
    }

    private byte[] readResource(String path) throws IOException {
        try (InputStream in = getClass().getResourceAsStream(path)) {
            return in.readAllBytes();
        }
    }

    private static String sign(byte[] body) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(SECRET.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            return "sha256=" + HexFormat.of().formatHex(mac.doFinal(body));
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }
}
