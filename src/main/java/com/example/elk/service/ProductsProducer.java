package com.example.elk.service;

import com.example.elk.model.ProductEvent;
import lombok.RequiredArgsConstructor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import java.util.*;

@Service
@RequiredArgsConstructor
public class ProductsProducer {
    private final KafkaTemplate<String, ProductEvent> kafkaTemplate;
    private final Logger logger = LogManager.getLogger(ProductsProducer.class);

    @Scheduled(fixedDelay = 10)
    public void sentEventToUpdatePrice(){
        Random random = new Random();
        logger.info("Starting producing event");
        kafkaTemplate.send("update-product-price", new ProductEvent("PL1111111116", "tesla", random.nextDouble() * 1000));
        kafkaTemplate.send("update-product-price", new ProductEvent("PL1111111113", "tesla", random.nextDouble() * 1000));
        kafkaTemplate.send("update-product-price", new ProductEvent("PL1111111114", "tesla", random.nextDouble() * 1000));
        kafkaTemplate.send("update-product-price", new ProductEvent("PL1111111111", "tesla", random.nextDouble() * 1000));
        kafkaTemplate.send("update-product-price", new ProductEvent("PL1111111112", "tesla", random.nextDouble() * 1000));
        kafkaTemplate.send("update-product-price", new ProductEvent("PL1111111117", "tesla", random.nextDouble() * 1000));
        kafkaTemplate.send("update-product-price", new ProductEvent("PL1111111118", "tesla", random.nextDouble() * 1000));
    }
}
