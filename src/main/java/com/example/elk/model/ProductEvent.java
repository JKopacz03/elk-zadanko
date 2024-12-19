package com.example.elk.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ProductEvent implements Serializable {
    private static final long serialVersionUID = 1L;
    private String ISIN;
    private String type;
    private double price;
}
