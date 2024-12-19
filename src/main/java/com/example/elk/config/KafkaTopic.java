package com.example.elk.config;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
@RequiredArgsConstructor
public class KafkaTopic {

    @Bean
    public NewTopic configTopic() {
        return TopicBuilder.name("update-product-price")
                .build();
    }
}

