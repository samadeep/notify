package com.example.notification.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

import com.example.notification.model.NotificationEvent;

import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${app.kafka.topic.notifications}")
    private String notificationsTopic;

    @Value("${app.kafka.topic.notifications-dlt}")
    private String notificationsDltTopic;

    @Value("${spring.kafka.consumer.group-id}")
    private String consumerGroupId;

    @Bean(name = "producerFactory")
    public ProducerFactory<String, NotificationEvent> producerFactory() {
        // This hashmap stores the properties for the producer factory
        Map<String, Object> props = new HashMap<>();
        // This is the bootstrap servers for the producer factory   
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // This is the key serializer for the producer factory
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        // This is the value serializer for the producer factory
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        // Improve reliability by setting the acks to all
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean(name = "kafkaTemplate")
    public KafkaTemplate<String, NotificationEvent> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    @Bean(name = "consumerFactory")
    public ConsumerFactory<String, NotificationEvent> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        // Configure JSON deserializer via properties only (no setters)
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, NotificationEvent.class.getName());
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "com.example.notification.*");
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Value("${spring.kafka.listener.concurrency:3}")
    private Integer concurrency;

    @Bean(name = "kafkaListenerContainerFactory")   
    public ConcurrentKafkaListenerContainerFactory<String, NotificationEvent> kafkaListenerContainerFactory(
            KafkaTemplate<String, NotificationEvent> kafkaTemplate) {
        ConcurrentKafkaListenerContainerFactory<String, NotificationEvent> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        // Concurrency is controlled via spring.kafka.listener.concurrency
        factory.setCommonErrorHandler(errorHandler(kafkaTemplate));
        factory.setConcurrency(concurrency);
        factory.getContainerProperties().setObservationEnabled(true);
        return factory;
    }

    @Bean(name = "errorHandler")
    public DefaultErrorHandler errorHandler(KafkaTemplate<String, NotificationEvent> template) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template, (record, ex) -> {
            return new TopicPartition(notificationsDltTopic, record.partition());
        });
        ExponentialBackOffWithMaxRetries backoff = new ExponentialBackOffWithMaxRetries(5);
        backoff.setInitialInterval(500L);
        backoff.setMultiplier(2.0);
        backoff.setMaxInterval(10_000L);
        return new DefaultErrorHandler(recoverer, backoff);
    }

    @Bean(name = "notificationsTopic")
    public NewTopic notificationsTopic() {
        // Scale partitions to increase consumer parallelism
        return new NewTopic(notificationsTopic, 6, (short) 1);
    }

    @Bean(name = "notificationsDltTopic")
    public NewTopic notificationsDltTopic() {
        return new NewTopic(notificationsDltTopic, 6, (short) 1);
    }
}


