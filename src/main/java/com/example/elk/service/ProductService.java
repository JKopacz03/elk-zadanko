package com.example.elk.service;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.FieldSort;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.aggregations.*;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import com.example.elk.model.Product;
import com.example.elk.model.ProductEvent;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class ProductService {
    private final KafkaTemplate<String, ProductEvent> kafkaTemplate;
    private final ElasticsearchClient elasticsearchClient;
    private final Logger logger = LogManager.getLogger(ProductService.class);

    @Scheduled(fixedDelay = 10)
    public void sentEventToUpdatePrice(){
        Random random = new Random();
        double newPrice = random.nextDouble() * 10000;
//        logger.info("New price: {}", newPrice);
        kafkaTemplate.send("update-product-price", new ProductEvent("PL1111111116", "tesla", random.nextDouble() * 1000));
        kafkaTemplate.send("update-product-price", new ProductEvent("PL1111111113", "tesla", random.nextDouble() * 1000));
        kafkaTemplate.send("update-product-price", new ProductEvent("PL1111111114", "tesla", random.nextDouble() * 1000));
        kafkaTemplate.send("update-product-price", new ProductEvent("PL1111111111", "tesla", random.nextDouble() * 1000));
        kafkaTemplate.send("update-product-price", new ProductEvent("PL1111111112", "tesla", random.nextDouble() * 1000));
        kafkaTemplate.send("update-product-price", new ProductEvent("PL1111111117", "tesla", random.nextDouble() * 1000));
        kafkaTemplate.send("update-product-price", new ProductEvent("PL1111111118", "tesla", random.nextDouble() * 1000));
    }

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

    public List<Product> getProduct(String ISIN) throws IOException {
        SearchResponse<Product> response = elasticsearchClient.search(searchRequest -> searchRequest
                .index("products")
                .query(queryBuilder ->
                    queryBuilder.match(matchQBuilder->
                        matchQBuilder.field("isin")
                           .query(ISIN))), Product.class
        );

        return response.hits().hits().stream()
                .map(Hit::source)
                .collect(Collectors.toList());
    }

    public Map<Double, Long> getPriceHistogram() throws IOException {
        SearchResponse<Void> response = elasticsearchClient.search(s -> s
                        .index("products")
                        .aggregations("price-histogram", a -> a
                                .histogram(h -> h
                                        .field("price")
                                        .interval(50.0)
                                )
                                .aggregations("limited-buckets", subAgg -> subAgg
                                        .bucketSort(bs -> bs
                                                .sort(sort -> sort
                                                        .field(f -> f.field("_count"))
                                                )
                                                .size(5)
                                        )
                                )
                        ),
                Void.class
        );

        List<HistogramBucket> buckets = response.aggregations()
                .get("price-histogram")
                .histogram()
                .buckets().array();

        Map<Double, Long> histogram = new HashMap<>();
        for (HistogramBucket bucket: buckets) {
            histogram.put(bucket.key(), bucket.docCount());

        }

        return histogram;
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
