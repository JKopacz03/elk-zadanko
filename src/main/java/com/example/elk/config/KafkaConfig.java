package com.example.elk.config;

import com.example.elk.model.ProductEvent;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.Collections;
import java.util.Properties;

@Configuration
@RequiredArgsConstructor
public class KafkaConfig {

    @Bean
    public NewTopic configTopic() {
        return TopicBuilder.name("update-product-price")
                .build();
    }

    @Bean(name = "update-product-price-consumer")
    public KafkaConsumer<String, ProductEvent> updateProductPriceConsumer(){
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:29092");
        props.put("group.id", "productConsumer");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.springframework.kafka.support.serializer.JsonDeserializer");
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        KafkaConsumer<String, ProductEvent> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("update-product-price"));
        return consumer;
    }
}

