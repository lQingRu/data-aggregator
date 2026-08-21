package com.dataaggregator.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public record SearchRequestCreateRequest(
        @NotBlank String workflow,
        @NotBlank String keywords,
        @NotBlank String question,
        @Valid @NotNull @JsonProperty("retrieval_filters") SearchRequestRetrievalFilters retrievalFilters,
        @Valid @NotNull @JsonProperty("initial_sort") SearchRequestInitialSort initialSort) {}
