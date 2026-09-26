package com.owencity.n8nkafka.webhook;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.assertj.MockMvcTester;
import org.springframework.test.web.servlet.assertj.MvcTestResult;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.HexFormat;

import static org.assertj.core.api.Assertions.assertThat;

@WebMvcTest(GithubWebhookController.class)
@Import(GithubSignatureVerifier.class)
// @WebMvcTest 슬라이스에는 @ConfigurationPropertiesScan이 적용되지 않는다.
@EnableConfigurationProperties(GithubWebhookProperties.class)
@TestPropertySource(properties = "github.webhook.secret=" + GithubWebhookControllerTest.SECRET)
class GithubWebhookControllerTest {

    static final String SECRET = "test-secret";
    private static final String DELIVERY_ID = "72d3162e-cc78-11e3-81ab-4c9367dc0958";

    @Autowired
    private MockMvcTester mvc;

    @ParameterizedTest
    @ValueSource(strings = {"opened", "synchronize", "reopened"})
    void acceptsTargetPullRequestActions(String action) throws IOException {
        String body = pullRequestPayload(action);

        assertThat(post("pull_request", DELIVERY_ID, body, sign(body))).hasStatus(202);
    }

    @ParameterizedTest
    @ValueSource(strings = {"closed", "edited", "labeled"})
    void ignoresOtherPullRequestActions(String action) throws IOException {
        String body = pullRequestPayload(action);

        assertThat(post("pull_request", DELIVERY_ID, body, sign(body))).hasStatus(204);
    }

    @Test
    void ignoresOtherEvents() {
        String body = "{\"ref\":\"refs/heads/main\"}";

        assertThat(post("push", DELIVERY_ID, body, sign(body))).hasStatus(204);
    }

    @Test
    void respondsToPing() {
        String body = "{\"zen\":\"Keep it logically awesome.\",\"hook_id\":1}";

        assertThat(post("ping", DELIVERY_ID, body, sign(body))).hasStatus(200);
    }

    @Test
    void rejectsInvalidSignature() throws IOException {
        String body = pullRequestPayload("opened");
        String signedWithOtherSecret = sign(body, "wrong-secret");

        assertThat(post("pull_request", DELIVERY_ID, body, signedWithOtherSecret)).hasStatus(401);
    }

    @Test
    void rejectsMissingSignature() throws IOException {
        assertThat(post("pull_request", DELIVERY_ID, pullRequestPayload("opened"), null)).hasStatus(401);
    }

    @Test
    void rejectsSignatureOfDifferentBody() throws IOException {
        String signedBody = pullRequestPayload("opened");
        String sentBody = signedBody.replace("\"number\": 4", "\"number\": 5");

        assertThat(post("pull_request", DELIVERY_ID, sentBody, sign(signedBody))).hasStatus(401);
    }

    @Test
    void rejectsSignedButMalformedJson() {
        String body = "{not json";

        assertThat(post("pull_request", DELIVERY_ID, body, sign(body))).hasStatus(400);
    }

    @Test
    void rejectsSignedPayloadMissingRequiredField() throws IOException {
        String body = pullRequestPayload("opened")
                .replace("\"sha\": \"6dcb09b5b57875f334f61aebed695e2e4193db5e\"", "\"sha\": \"\"");

        assertThat(post("pull_request", DELIVERY_ID, body, sign(body))).hasStatus(400);
    }

    @Test
    void rejectsMissingDeliveryId() throws IOException {
        String body = pullRequestPayload("opened");

        assertThat(post("pull_request", null, body, sign(body))).hasStatus(400);
    }

    private MvcTestResult post(String event, String deliveryId, String body, String signature) {
        var request = mvc.post().uri("/webhooks/github")
                .contentType(MediaType.APPLICATION_JSON)
                .header("X-GitHub-Event", event)
                .content(body);
        if (deliveryId != null) {
            request.header("X-GitHub-Delivery", deliveryId);
        }
        if (signature != null) {
            request.header("X-Hub-Signature-256", signature);
        }
        return request.exchange();
    }

    private String pullRequestPayload(String action) throws IOException {
        try (InputStream in = getClass().getResourceAsStream("/webhook/pull_request.synchronize.json")) {
            String json = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            return json.replace("\"action\": \"synchronize\"", "\"action\": \"" + action + "\"");
        }
    }

    // 운영 코드(GithubSignatureVerifier)를 재사용하지 않고 독립적으로 서명을 계산한다.
    private static String sign(String body) {
        return sign(body, SECRET);
    }

    private static String sign(String body, String secret) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            return "sha256=" + HexFormat.of().formatHex(mac.doFinal(body.getBytes(StandardCharsets.UTF_8)));
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }
}
