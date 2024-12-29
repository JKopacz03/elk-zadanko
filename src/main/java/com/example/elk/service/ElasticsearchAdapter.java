package com.example.elk.service;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.aggregations.HistogramBucket;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import com.example.elk.model.Product;
import lombok.RequiredArgsConstructor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class ElasticsearchAdapter {
    private final ElasticsearchClient elasticsearchClient;
    private final Logger logger = LogManager.getLogger(ProductsConsumer.class);

    public void saveOrUpdateProduct(Product product) throws IOException {
        IndexResponse response = elasticsearchClient.index(i -> i
                .index("products")
                .id(product.getISIN())
                .document(product)
        );
        logger.info("Indexed with version {}", response.version());
    }

    public List<Product> getProduct(String ISIN) throws IOException {
        logger.info("Get product by ISIN: {}", ISIN);
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
        logger.info("Get price histogram");
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
}
