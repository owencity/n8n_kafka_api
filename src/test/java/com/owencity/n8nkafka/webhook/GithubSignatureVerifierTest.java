package com.owencity.n8nkafka.webhook;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class GithubSignatureVerifierTest {

    // GitHub 공식 문서의 테스트 벡터
    // https://docs.github.com/en/webhooks/using-webhooks/validating-webhook-deliveries#testing-the-webhook-payload-validation
    private static final String SECRET = "It's a Secret to Everybody";
    private static final byte[] BODY = "Hello, World!".getBytes(StandardCharsets.UTF_8);
    private static final String SIGNATURE = "sha256=757107ea0eb2509fc211221cce984b8a37570b6d7586c22c46f4379c8b043e17";

    private final GithubSignatureVerifier verifier = new GithubSignatureVerifier(new GithubWebhookProperties(SECRET));

    @Test
    void acceptsGithubDocumentedSignature() {
        assertThat(verifier.isValid(BODY, SIGNATURE)).isTrue();
    }

    @Test
    void acceptsUppercaseHex() {
        String upper = "sha256=" + SIGNATURE.substring("sha256=".length()).toUpperCase();

        assertThat(verifier.isValid(BODY, upper)).isTrue();
    }

    @Test
    void rejectsTamperedBody() {
        byte[] tampered = "Hello, World?".getBytes(StandardCharsets.UTF_8);

        assertThat(verifier.isValid(tampered, SIGNATURE)).isFalse();
    }

    @Test
    void rejectsSignatureMadeWithDifferentSecret() {
        GithubSignatureVerifier otherSecret = new GithubSignatureVerifier(new GithubWebhookProperties("another-secret"));

        assertThat(otherSecret.isValid(BODY, SIGNATURE)).isFalse();
    }

    @Test
    void rejectsMissingOrMalformedHeader() {
        assertThat(verifier.isValid(BODY, null)).isFalse();
        assertThat(verifier.isValid(BODY, "")).isFalse();
        assertThat(verifier.isValid(BODY, SIGNATURE.substring("sha256=".length()))).isFalse();
        assertThat(verifier.isValid(BODY, "sha1=757107ea0eb2509fc211221cce984b8a37570b6d")).isFalse();
        assertThat(verifier.isValid(BODY, "sha256=not-hex")).isFalse();
        assertThat(verifier.isValid(BODY, "sha256=")).isFalse();
    }

    @Test
    void toStringDoesNotExposeSecret() {
        assertThat(new GithubWebhookProperties(SECRET).toString()).doesNotContain(SECRET);
    }
}
