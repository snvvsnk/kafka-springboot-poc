package com.naveen.kafkapoc.kafkademo.consumer;

import com.naveen.kafkapoc.kafkademo.model.OrderEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class OrderDltConsumer {
    private static final Logger log = LoggerFactory.getLogger(OrderDltConsumer.class);

    @KafkaListener(topics = "orders-topic.DLT", groupId = "poc-consumer-dlt-group")
    public void handleDltMessage(OrderEvent orderEvent) {
        log.info("Received Order DLT message: {}", orderEvent);
    }
}
