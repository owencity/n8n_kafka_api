package com.owencity.n8nkafka.webhook;

import org.springframework.stereotype.Component;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.util.HexFormat;

/**
 * {@code X-Hub-Signature-256} 헤더를 검증한다.
 *
 * <p>GitHub는 webhook secret을 key로 요청 body 원문의 HMAC-SHA256을 계산해 {@code sha256=<hex>} 형태로 보낸다.
 * JSON을 파싱/재직렬화하면 바이트가 달라지므로 반드시 수신한 body 바이트 그대로 검증한다.
 */
@Component
public class GithubSignatureVerifier {

    private static final String ALGORITHM = "HmacSHA256";
    private static final String PREFIX = "sha256=";

    private final SecretKeySpec key;

    public GithubSignatureVerifier(GithubWebhookProperties properties) {
        this.key = new SecretKeySpec(properties.secret().getBytes(StandardCharsets.UTF_8), ALGORITHM);
    }

    public boolean isValid(byte[] body, String signatureHeader) {
        if (signatureHeader == null || !signatureHeader.startsWith(PREFIX)) {
            return false;
        }
        byte[] expected;
        try {
            expected = HexFormat.of().parseHex(signatureHeader.substring(PREFIX.length()));
        } catch (IllegalArgumentException e) {
            return false;
        }
        // 일반 equals는 앞에서부터 다른 바이트를 만나면 바로 반환해 응답 시간으로 정답이 새어 나갈 수 있다.
        return MessageDigest.isEqual(hmac(body), expected);
    }

    // Mac은 스레드 안전하지 않으므로 요청마다 새로 만든다.
    private byte[] hmac(byte[] body) {
        try {
            Mac mac = Mac.getInstance(ALGORITHM);
            mac.init(key);
            return mac.doFinal(body);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("HmacSHA256을 사용할 수 없다", e);
        }
    }
}
