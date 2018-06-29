package com.falabella.kafka;

import com.falabella.domain.Item;
import com.falabella.domain.Product;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class KafkaMsgListener {
    @KafkaListener(topics = "test_topic", groupId = "product-consumer")
    public void processProductMessage(Product message, Acknowledgment ack) {
        System.out.println("Product Kafka Message: " + message.toString());
        ack.acknowledge();
    }

    @KafkaListener(topics = "test_topic", groupId = "item-consumer")
    public void processItemMessage(Item message, Acknowledgment ack) throws Exception {
        System.out.println("Item Kafka Message: " + message.toString());
        throw new Exception("Timed Out trying to process message");
        //ack.acknowledge();
    }

}
