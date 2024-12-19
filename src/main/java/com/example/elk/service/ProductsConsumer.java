package com.example.elk.service;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import com.example.elk.model.Product;
import com.example.elk.model.ProductEvent;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Service
@RequiredArgsConstructor
public class ProductsConsumer {
    private final ElasticsearchClient elasticsearchClient;
    private final Logger logger = LogManager.getLogger(ProductsConsumer.class);

    @Scheduled(fixedDelay = 5000)
    public void updatePrice() throws IOException {
        try (KafkaConsumer<String, ProductEvent> consumer = new KafkaConsumer<>(getProps())) {
            consumer.subscribe(Collections.singletonList("update-product-price"));
            ConsumerRecords<String, ProductEvent> records = consumer.poll(Duration.ofSeconds(5));
            for (ConsumerRecord<String, ProductEvent> record : records) {
                ProductEvent event = record.value();
                logger.info("Received event: {}", event);
                saveOrUpdateProduct(new Product(event.getISIN(), event.getType(), event.getPrice()));
            }
        }
    }

    public void saveOrUpdateProduct(Product product) throws IOException {
        IndexResponse response = elasticsearchClient.index(i -> i
                .index("products")
                .id(product.getISIN())
                .document(product)
        );
        logger.info("Indexed with version {}", response.version());
    }

    private Properties getProps() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:29092");
        props.put("group.id", "productConsumer");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.springframework.kafka.support.serializer.JsonDeserializer");
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        return props;
    }
}
