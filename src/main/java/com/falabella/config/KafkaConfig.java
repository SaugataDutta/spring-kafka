package com.falabella.config;

import com.falabella.exception.RetriableException;
import com.falabella.exception.TerminalException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.AbstractMessageListenerContainer.AckMode;
import org.springframework.kafka.listener.ConsumerAwareListenerErrorHandler;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.MessageHeaders;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.util.concurrent.ListenableFuture;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Configuration
@EnableKafka
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapAddress;
    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapAddress);
        props.put(
                ConsumerConfig.GROUP_ID_CONFIG,
                groupId);
        props.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class);
        props.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String>
    kafkaListenerContainerFactory() {

        ConcurrentKafkaListenerContainerFactory<String, String> factory
                = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        //factory.setMessageConverter(new StringJsonMessageConverter());
        factory.getContainerProperties().setAckMode(AckMode.MANUAL_IMMEDIATE);
        factory.setConcurrency(1);
        factory.setRetryTemplate(retryTemplate());
        factory.setReplyTemplate(replyTemplate());
        return factory;
    }

    @Bean
    public RetryTemplate retryTemplate() {
        RetryTemplate retryTemplate = new RetryTemplate();

        FixedBackOffPolicy fixedBackOffPolicy = new FixedBackOffPolicy();
        fixedBackOffPolicy.setBackOffPeriod(5000l);
        retryTemplate.setBackOffPolicy(fixedBackOffPolicy);

        //Map<Class<? extends Throwable>, Boolean> retryableExceptions = new HashMap<>();
        //retryableExceptions.put(RuntimeException.class, true);
        //retryableExceptions.put(IOException.class, true);
        //retryableExceptions.put(JsonProcessingException.class,false);
        //SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy(5, retryableExceptions);
        SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy();
        retryPolicy.setMaxAttempts(5);
        retryTemplate.setRetryPolicy(retryPolicy);

        return retryTemplate;
    }

    @Bean
    public ConsumerAwareListenerErrorHandler itemAwareErrorHandler() {
        return (m, e, c) -> {
            //this.listen3Exception = e;
            MessageHeaders headers = m.getHeaders();
            if (e.getRootCause() instanceof RetriableException) {
                log.info("Retrying Failed Message: " + headers.get(KafkaHeaders.OFFSET, Long
                        .class));
                c.seek(new org.apache.kafka.common.TopicPartition(
                                headers.get(KafkaHeaders.RECEIVED_TOPIC, String.class),
                                headers.get(KafkaHeaders.RECEIVED_PARTITION_ID, Integer.class)),
                        headers.get(KafkaHeaders.OFFSET, Long.class));
                return null;
            } else if (e.getRootCause() instanceof TerminalException) {
                log.info("Logging Failed Message to Error Queue: " + headers.get(KafkaHeaders
                                .OFFSET,
                        Long
                                .class));
                return m.getPayload();
            } else {
                return null;
            }

        };
    }

    @Bean
    public KafkaTemplate<String, String> replyTemplate() {
        return new KafkaTemplate<String, String>(producerFactory()) {
            @Override
            public ListenableFuture<SendResult<String, String>> send(String topic, String data) {
                if (null != data) {
                    return super.send(topic, data);
                } else {
                    return null;
                }
            }
        };
    }
}
