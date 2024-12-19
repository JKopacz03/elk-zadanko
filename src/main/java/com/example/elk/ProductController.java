package com.example.elk;

import co.elastic.clients.elasticsearch._types.aggregations.HistogramAggregate;
import com.example.elk.model.Product;
import com.example.elk.service.ProductService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/product")
@RequiredArgsConstructor
public class ProductController {
    private final ProductService productService;

    @GetMapping("/{ISIN}")
    public ResponseEntity<List<Product>> getProductByISIN(@PathVariable String ISIN) throws IOException {
        return new ResponseEntity<>(productService.getProduct(ISIN), HttpStatus.OK);
    }

    @GetMapping("/price-histogram")
    public ResponseEntity<Map<Double, Long>> getPriceHistogram() throws IOException {
        return new ResponseEntity<>(productService.getPriceHistogram(), HttpStatus.OK);
    }
}
