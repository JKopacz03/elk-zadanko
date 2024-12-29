package com.example.elk.service;

import com.example.elk.model.Product;
import com.example.elk.model.ProductEvent;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.Duration;

@Service
@RequiredArgsConstructor
public class ProductsConsumer {
    private final ElasticsearchAdapter elasticsearchAdapter;
    private final Logger logger = LogManager.getLogger(ProductsConsumer.class);
    @Qualifier("update-product-price-consumer")
    private final KafkaConsumer<String, ProductEvent> kafkaConsumer;

    @Scheduled(fixedDelay = 5000)
    public void updatePrice() throws IOException {
        ConsumerRecords<String, ProductEvent> records = kafkaConsumer.poll(Duration.ofSeconds(5));
        for (ConsumerRecord<String, ProductEvent> record : records) {
            ProductEvent event = record.value();
            logger.info("Received event: {}", event);
            elasticsearchAdapter.saveOrUpdateProduct(new Product(event.getISIN(), event.getType(), event.getPrice()));
        }
    }
}
