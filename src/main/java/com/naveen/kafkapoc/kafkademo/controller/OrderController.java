package com.naveen.kafkapoc.kafkademo.controller;

import com.naveen.kafkapoc.kafkademo.model.OrderEvent;
import com.naveen.kafkapoc.kafkademo.producer.OrderProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/orders")
public class OrderController {
    private final OrderProducer producer;

    public OrderController(OrderProducer producer) {
        this.producer = producer;
    }

    @PostMapping
    public ResponseEntity<String>  createOrder(@RequestBody OrderEvent order) {
        producer.sendOrder(order);
        return ResponseEntity.ok("Order sent: " + order.orderId());
    }
}
