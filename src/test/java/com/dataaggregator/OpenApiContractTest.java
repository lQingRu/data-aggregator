package com.dataaggregator;

import static org.assertj.core.api.Assertions.assertThat;

import com.dataaggregator.support.IntegrationTestContainers;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.ResponseEntity;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class OpenApiContractTest extends IntegrationTestContainers {

    @LocalServerPort
    private int port;

    @Autowired
    private TestRestTemplate restTemplate;

    @Test
    void generatedOpenApiContractExposesPhaseOneApiSurface() {
        ResponseEntity<Map> response =
                restTemplate.getForEntity("http://localhost:" + port + "/v3/api-docs", Map.class);

        assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();
        Map<?, ?> document = response.getBody();

        assertThat(document).isNotNull();
        assertThat(document.get("openapi")).isNotNull();
        assertThat(document.get("paths")).isNotNull();
        assertThat(document.get("components")).isNotNull();

        Map<?, ?> paths = mapValue(document, "paths");
        assertContainsKeys(
                paths,
                "/search-requests",
                "/operations/{operationId}",
                "/operations/{operationId}/cancel",
                "/events",
                "/result-snapshots/{snapshotId}",
                "/result-snapshots/{snapshotId}/activity",
                "/result-snapshots/{snapshotId}/schema",
                "/result-snapshots/{snapshotId}/query");
        assertThat(paths.containsKey("/internal/runtime")).isFalse();

        Map<?, ?> components = mapValue(document, "components");
        Map<?, ?> schemas = mapValue(components, "schemas");
        assertContainsKeys(
                schemas,
                "SearchRequestCreateRequest",
                "SearchRequestCreateResponse",
                "OperationResponse",
                "SnapshotMetadataResponse",
                "SnapshotActivityResponse",
                "SnapshotSchemaResponse",
                "SnapshotQueryRequest",
                "SnapshotQueryResponse",
                "ApiErrorResponse");
        assertThat(mapValue(components, "securitySchemes").containsKey("MockUserId"))
                .isTrue();

        assertReferencesExist(document, document);
    }

    @Test
    void swaggerUiEndpointIsAvailable() {
        ResponseEntity<String> response =
                restTemplate.getForEntity("http://localhost:" + port + "/swagger-ui.html", String.class);

        assertThat(response.getStatusCode().is3xxRedirection()
                        || response.getStatusCode().is2xxSuccessful())
                .isTrue();
        if (response.getHeaders().getLocation() == null) {
            assertThat(response.getBody()).containsIgnoringCase("swagger");
        } else {
            assertThat(response.getHeaders().getLocation().toString()).contains("/swagger-ui/");
        }
    }

    private void assertReferencesExist(Object node, Map<?, ?> document) {
        if (node instanceof Map<?, ?> map) {
            Object ref = map.get("$ref");
            if (ref instanceof String value) {
                assertThat(resolveRef(document, value)).as(value).isNotNull();
            }
            map.values().forEach(value -> assertReferencesExist(value, document));
        } else if (node instanceof Iterable<?> values) {
            values.forEach(value -> assertReferencesExist(value, document));
        }
    }

    private Object resolveRef(Map<?, ?> document, String ref) {
        assertThat(ref).startsWith("#/");
        Object current = document;
        for (String segment : ref.substring(2).split("/")) {
            if (!(current instanceof Map<?, ?> map)) {
                return null;
            }
            current = map.get(segment);
        }
        return current;
    }

    private Map<?, ?> mapValue(Map<?, ?> map, String key) {
        Object value = map.get(key);
        assertThat(value).isInstanceOf(Map.class);
        return (Map<?, ?>) value;
    }

    private void assertContainsKeys(Map<?, ?> map, String... keys) {
        for (String key : keys) {
            assertThat(map.containsKey(key)).as(key).isTrue();
        }
    }
}
