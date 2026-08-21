package com.dataaggregator.api;

import java.util.Map;

public record ApiErrorResponse(String code, String message, int status, String path, Map<String, Object> details) {

    public ApiErrorResponse {
        details = details == null ? Map.of() : Map.copyOf(details);
    }
}
