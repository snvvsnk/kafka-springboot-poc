package com.naveen.kafkapoc.kafkademo.producer;

import com.naveen.kafkapoc.kafkademo.model.OrderEvent;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class OrderProducer {

    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

    public OrderProducer(KafkaTemplate<String, OrderEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendOrder(OrderEvent order) {
        kafkaTemplate.send("orders-topic", order.orderId(), order);
    }
}
