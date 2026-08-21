package com.dataaggregator;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.yaml.snakeyaml.Yaml;

class OpenApiContractTest {

    @Test
    void openApiContractParsesAndReferencesExistingComponents() throws IOException {
        Object parsed = new Yaml().load(Files.readString(Path.of("docs/specs/openapi.yaml")));

        assertThat(parsed).isInstanceOf(Map.class);
        Map<?, ?> document = (Map<?, ?>) parsed;
        assertThat(document.get("openapi")).isNotNull();
        assertThat(document.get("paths")).isNotNull();
        assertThat(document.get("components")).isNotNull();

        Map<?, ?> components = mapValue(document, "components");
        Map<?, ?> schemas = mapValue(components, "schemas");
        assertThat(schemas).isNotEmpty();

        assertReferencesExist(document, document);
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
}
