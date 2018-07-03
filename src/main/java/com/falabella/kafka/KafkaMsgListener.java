package com.falabella.kafka;

import com.falabella.domain.Item;
import com.falabella.exception.RetriableException;
import com.falabella.exception.TerminalException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
@Slf4j
public class KafkaMsgListener {
    @KafkaListener(topics = "test_topic", groupId = "item-consumer", errorHandler =
            "itemAwareErrorHandler")
    @SendTo("error_topic")
    public void itemMessageListener(String message, Acknowledgment ack) throws Exception {
        try {
            Item item = new ObjectMapper().readValue(message, Item.class);
            log.info("Item Kafka Message: " + item.toString());
            processItem(item);
            ack.acknowledge();
        } catch (IOException ie) {
            log.error("Product JSON Serialization Error: ");
            ack.acknowledge();
            throw new TerminalException("Forced Exception");
        }
    }

    public void processItem(Item item) throws RetriableException{
        throw new RetriableException(item.getName());
    }

}
