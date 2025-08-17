package com.example.notification.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import com.example.notification.model.NotificationEvent;

import static org.springframework.kafka.support.KafkaHeaders.DLT_EXCEPTION_FQCN;
import static org.springframework.kafka.support.KafkaHeaders.DLT_EXCEPTION_MESSAGE;
import static org.springframework.kafka.support.KafkaHeaders.DLT_ORIGINAL_TOPIC;
import static org.springframework.kafka.support.KafkaHeaders.DLT_ORIGINAL_PARTITION;
import static org.springframework.kafka.support.KafkaHeaders.DLT_ORIGINAL_OFFSET;

@Service
public class DeadLetterConsumer {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterConsumer.class);

    @KafkaListener(topics = "${app.kafka.topic.notifications-dlt}", groupId = "${spring.kafka.consumer.group-id}", containerFactory = "kafkaListenerContainerFactory")
    public void onDeadLetter(
            @Payload NotificationEvent event,
            @Header(name = DLT_EXCEPTION_FQCN, required = false) String exceptionClass,
            @Header(name = DLT_EXCEPTION_MESSAGE, required = false) String exceptionMessage,
            @Header(name = DLT_ORIGINAL_TOPIC, required = false) String originalTopic,
            @Header(name = DLT_ORIGINAL_PARTITION, required = false) Integer originalPartition,
            @Header(name = DLT_ORIGINAL_OFFSET, required = false) Long originalOffset
    ) {
        log.error("Dead-lettered notification id={}, originalTopic={}, partition={}, offset={}, exClass={}, exMessage={}",
                event != null ? event.getId() : null, originalTopic, originalPartition, originalOffset, exceptionClass, exceptionMessage);
        // TODO: persist to a database / alerting system for manual action
    }
}


