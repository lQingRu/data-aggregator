package com.dataaggregator.api;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.ConstraintViolationException;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import org.springframework.web.server.ResponseStatusException;

@RestControllerAdvice
public class ApiExceptionHandler {

    private final ObjectMapper objectMapper;

    public ApiExceptionHandler(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @ExceptionHandler(ResponseStatusException.class)
    public ResponseEntity<?> responseStatus(ResponseStatusException exception, HttpServletRequest request) {
        HttpStatus status = HttpStatus.valueOf(exception.getStatusCode().value());
        String message = exception.getReason() == null ? status.getReasonPhrase() : exception.getReason();
        return response(error(status.name(), message, status, request, Map.of()), request);
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<?> validation(MethodArgumentNotValidException exception, HttpServletRequest request) {
        Map<String, Object> details = new LinkedHashMap<>();
        Map<String, String> fieldErrors = new LinkedHashMap<>();
        exception
                .getBindingResult()
                .getFieldErrors()
                .forEach(error -> fieldErrors.put(
                        error.getField(),
                        error.getDefaultMessage() == null ? "Invalid value" : error.getDefaultMessage()));
        Map<String, String> objectErrors = new LinkedHashMap<>();
        exception
                .getBindingResult()
                .getGlobalErrors()
                .forEach(error -> objectErrors.put(
                        error.getObjectName(),
                        error.getDefaultMessage() == null ? "Invalid value" : error.getDefaultMessage()));
        if (!fieldErrors.isEmpty()) {
            details.put("field_errors", fieldErrors);
        }
        if (!objectErrors.isEmpty()) {
            details.put("object_errors", objectErrors);
        }
        return response(
                error("VALIDATION_ERROR", "Request validation failed", HttpStatus.BAD_REQUEST, request, details),
                request);
    }

    @ExceptionHandler({
        HttpMessageNotReadableException.class,
        ConstraintViolationException.class,
        MissingServletRequestParameterException.class,
        MethodArgumentTypeMismatchException.class
    })
    public ResponseEntity<?> badRequest(Exception exception, HttpServletRequest request) {
        return response(
                error("BAD_REQUEST", exception.getMessage(), HttpStatus.BAD_REQUEST, request, Map.of()), request);
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<?> serverError(Exception exception, HttpServletRequest request) {
        return response(
                error(
                        "INTERNAL_SERVER_ERROR",
                        "Unexpected server failure",
                        HttpStatus.INTERNAL_SERVER_ERROR,
                        request,
                        Map.of()),
                request);
    }

    private ResponseEntity<?> response(ApiErrorResponse error, HttpServletRequest request) {
        if (acceptsEventStream(request)) {
            return ResponseEntity.status(error.status())
                    .contentType(MediaType.TEXT_EVENT_STREAM)
                    .body(eventStreamError(error));
        }
        return ResponseEntity.status(error.status()).body(error);
    }

    private boolean acceptsEventStream(HttpServletRequest request) {
        String accept = request.getHeader("Accept");
        if (accept == null || accept.isBlank()) {
            return false;
        }
        return MediaType.parseMediaTypes(accept).stream()
                .anyMatch(mediaType -> mediaType.isCompatibleWith(MediaType.TEXT_EVENT_STREAM)
                        && "text".equals(mediaType.getType())
                        && "event-stream".equals(mediaType.getSubtype()));
    }

    private String eventStreamError(ApiErrorResponse error) {
        try {
            return "event: error\ndata: " + objectMapper.writeValueAsString(error) + "\n\n";
        } catch (JsonProcessingException exception) {
            return "event: error\ndata: {\"code\":\"INTERNAL_SERVER_ERROR\",\"message\":\"Unexpected server failure\","
                    + "\"status\":500,\"path\":\"" + error.path() + "\",\"details\":{}}\n\n";
        }
    }

    private ApiErrorResponse error(
            String code, String message, HttpStatus status, HttpServletRequest request, Map<String, Object> details) {
        return new ApiErrorResponse(code, message, status.value(), request.getRequestURI(), details);
    }
}
