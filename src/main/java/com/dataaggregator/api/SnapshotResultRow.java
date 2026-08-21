package com.dataaggregator.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public record SnapshotResultRow(
        @JsonProperty("chunk_id") String chunkId,
        @JsonProperty("parent_entity_id") String parentEntityId,
        @JsonProperty("parent_title") String parentTitle,
        @JsonProperty("parent_type") String parentType,
        @JsonProperty("source_name") String sourceName,
        String ticker,
        @JsonProperty("company_name") String companyName,
        String sector,
        String region,
        @JsonProperty("published_at") String publishedAt,
        String author,
        @JsonProperty("chunk_text") String chunkText,
        @JsonProperty("relevance_score") Double relevanceScore,
        @JsonProperty("lexical_rank") Integer lexicalRank,
        @JsonProperty("source_contributions") List<String> sourceContributions) {}
