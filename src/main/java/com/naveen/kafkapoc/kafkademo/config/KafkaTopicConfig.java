package com.naveen.kafkapoc.kafkademo.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
@EnableKafka
public class KafkaTopicConfig {

    @Bean
    public NewTopic ordersTopic() {
        return TopicBuilder.name("orders-topic")
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic ordersTopicDLT() {
        return TopicBuilder.name("orders-topic.DLT")
                .partitions(3)
                .replicas(1)
                .build();
    }

    // Error handler with DLT publishing and retry logic
    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<String, Object>  kafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (cr, e) -> new org.apache.kafka.common.TopicPartition("orders-topic.DLT", cr.partition())
        );

        // Retry 3 times with 1-second backoff between retries
        // After 3 failed attempts, message goes to DLT instead of blocking
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 3L));

        errorHandler.addNotRetryableExceptions(org.apache.kafka.common.errors.SerializationException.class);

        return errorHandler;



    }
}
