package com.owencity.n8nkafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class N8nKafkaApiApplication {

	public static void main(String[] args) {
		SpringApplication.run(N8nKafkaApiApplication.class, args);
	}

}
