package com.dataaggregator.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "data-aggregator")
public record DataAggregatorProperties(String runtimeMode, Auth auth, Worker worker, Workflow workflow) {

    public record Auth(String mockUserId) {}

    public record Worker(String placeholderQueue) {}

    public record Workflow(int hybridChunkSearchVersion) {}
}
