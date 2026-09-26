package com.owencity.n8nkafka;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(properties = "github.webhook.secret=test-secret")
class N8nKafkaApiApplicationTests {

	@Test
	void contextLoads() {
	}

}
