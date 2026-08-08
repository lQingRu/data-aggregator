package com.dataaggregator.api;

import com.dataaggregator.config.DataAggregatorProperties;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class RuntimeController {

    private final DataAggregatorProperties properties;

    public RuntimeController(DataAggregatorProperties properties) {
        this.properties = properties;
    }

    @GetMapping("/internal/runtime")
    public RuntimeDescriptor describeRuntime() {
        return new RuntimeDescriptor(
                "data-aggregator",
                properties.runtimeMode(),
                properties.auth().mockUserId(),
                properties.workflow().hybridChunkSearchVersion());
    }
}
