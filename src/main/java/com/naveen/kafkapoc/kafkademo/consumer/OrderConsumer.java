package com.naveen.kafkapoc.kafkademo.consumer;


import com.naveen.kafkapoc.kafkademo.model.OrderEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class OrderConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderConsumer.class);

    @KafkaListener(topics = "orders-topic", groupId = "poc-consumer-group")
    public void consume(OrderEvent order) {
        log.info("Received order: {}", order);
    }
}
