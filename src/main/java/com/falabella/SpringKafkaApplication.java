package com.falabella;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@EnableAutoConfiguration
public class SpringKafkaApplication {
    public static void main(String[] args) {
        SpringApplication.run(SpringKafkaApplication.class, args);
    }
}
