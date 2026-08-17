package com.dataaggregator.worker;

import java.time.Instant;

public record InvestmentResearchChunk(
        String chunkId,
        String parentEntityId,
        String parentTitle,
        String parentType,
        String sourceName,
        String ticker,
        String companyName,
        String sector,
        String region,
        Instant publishedAt,
        String author,
        int chunkIndex,
        String chunkText) {}
