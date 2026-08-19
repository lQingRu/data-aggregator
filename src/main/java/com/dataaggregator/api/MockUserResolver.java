package com.dataaggregator.api;

import com.dataaggregator.config.DataAggregatorProperties;
import org.springframework.stereotype.Component;

@Component
public class MockUserResolver {

    public static final String HEADER = "X-Mock-User-Id";

    private final DataAggregatorProperties properties;

    public MockUserResolver(DataAggregatorProperties properties) {
        this.properties = properties;
    }

    public String userId(String headerValue) {
        if (headerValue != null && !headerValue.isBlank()) {
            return headerValue;
        }
        return properties.auth().mockUserId();
    }
}
