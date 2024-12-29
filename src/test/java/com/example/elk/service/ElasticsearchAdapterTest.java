package com.example.elk.service;

import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.example.elk.model.ProductEvent;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(SpringExtension.class)
@SpringBootTest
@ActiveProfiles("test")
@Testcontainers
public class ElasticsearchAdapterTest {

    @Container
    private static final ElasticsearchContainer elasticsearchContainer = new ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch:8.10.2")
            .withEnv("discovery.type", "single-node")
            .withEnv("xpack.security.enabled", "false");

    @Container
    static final ConfluentKafkaContainer kafka = new ConfluentKafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.6.1")
    );

    @DynamicPropertySource
    static void overrideProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    private ElasticsearchAdapter elasticsearchAdapter;

    @BeforeEach
    void setUp() throws IOException {
        String httpHostAddress = elasticsearchContainer.getHttpHostAddress();
        RestClient restClient = RestClient
                .builder(HttpHost.create(httpHostAddress))
                .build();
        ElasticsearchTransport transport = new RestClientTransport(
                restClient, new JacksonJsonpMapper());
        ElasticsearchClient elasticsearchClient = new ElasticsearchClient(transport);
        elasticsearchAdapter = new ElasticsearchAdapter(elasticsearchClient);

        elasticsearchClient.indices().create(c -> c.index("products"));

        elasticsearchClient.index(i -> i
                .index("products")
                .id("1")
                .document(new ProductEvent("PL1111111111", "tesla", 100))
        );

        elasticsearchClient.index(i -> i
                .index("products")
                .id("2")
                .document(new ProductEvent("PL1111111112", "bmw", 150))
        );

        elasticsearchClient.index(i -> i
                .index("products")
                .id("3")
                .document(new ProductEvent("PL1111111113", "mercedes", 200))
        );

        elasticsearchClient.index(i -> i
                .index("products")
                .id("4")
                .document(new ProductEvent("PL1111111114", "spacex", 250))
        );

        elasticsearchClient.index(i -> i
                .index("products")
                .id("5")
                .document(new ProductEvent("PL1111111115", "ekipa friza", 300))
        );

        elasticsearchClient.indices().refresh(r -> r.index("products"));
    }

    @Test
    void testGetPriceHistogram() throws IOException {
        Map<Double, Long> histogram = elasticsearchAdapter.getPriceHistogram();

        assertThat(histogram).isNotEmpty();
        assertThat(histogram.keySet()).containsExactlyInAnyOrder(100.0, 150.0, 200.0, 250.0, 300.0);
        assertThat(histogram.values()).allMatch(count -> count == 1);
    }


}
