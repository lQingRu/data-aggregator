package com.dataaggregator;

import com.dataaggregator.config.DataAggregatorProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties(DataAggregatorProperties.class)
public class DataAggregatorApplication {

    public static void main(String[] args) {
        SpringApplication.run(DataAggregatorApplication.class, args);
    }
}
