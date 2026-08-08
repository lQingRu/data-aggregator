package com.dataaggregator;

import static org.assertj.core.api.Assertions.assertThat;

import com.dataaggregator.api.RuntimeDescriptor;
import com.dataaggregator.support.IntegrationTestContainers;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class ApiRuntimeIT extends IntegrationTestContainers {

    @LocalServerPort
    private int port;

    @Autowired
    private TestRestTemplate restTemplate;

    @Test
    void apiRuntimeStartsWithMockUserConfiguration() {
        RuntimeDescriptor runtime =
                restTemplate.getForObject("http://localhost:" + port + "/internal/runtime", RuntimeDescriptor.class);

        assertThat(runtime.application()).isEqualTo("data-aggregator");
        assertThat(runtime.runtimeMode()).isEqualTo("api");
        assertThat(runtime.mockUserId()).isEqualTo("user_test");
        assertThat(runtime.workflowConfigVersion()).isEqualTo(1);
    }
}
