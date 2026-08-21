package com.dataaggregator.api;

import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.enums.SecuritySchemeIn;
import io.swagger.v3.oas.annotations.enums.SecuritySchemeType;
import io.swagger.v3.oas.annotations.info.Info;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.security.SecurityScheme;
import org.springframework.context.annotation.Configuration;

@Configuration
@OpenAPIDefinition(
        info =
                @Info(
                        title = "Data Aggregator API",
                        version = "0.0.1",
                        description =
                                "Phase-one HTTP and SSE contract for Hybrid Chunk Search operations and Result Snapshots."),
        security = @SecurityRequirement(name = "MockUserId"))
@SecurityScheme(
        name = "MockUserId",
        type = SecuritySchemeType.APIKEY,
        in = SecuritySchemeIn.HEADER,
        paramName = MockUserResolver.HEADER,
        description = "Phase-one mock user identity header.")
public class OpenApiConfiguration {}
