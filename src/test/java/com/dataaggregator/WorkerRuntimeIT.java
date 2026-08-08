package com.dataaggregator;

import static org.assertj.core.api.Assertions.assertThat;

import com.dataaggregator.config.DataAggregatorProperties;
import com.dataaggregator.support.IntegrationTestContainers;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(
        properties = {
            "spring.profiles.active=worker",
            "spring.main.web-application-type=none",
        })
class WorkerRuntimeIT extends IntegrationTestContainers {

    @Autowired
    private AmqpAdmin amqpAdmin;

    @Autowired
    private DataAggregatorProperties properties;

    @Test
    void workerRuntimeStartsAndDeclaresPlaceholderQueue() {
        assertThat(properties.runtimeMode()).isEqualTo("worker");
        assertThat(amqpAdmin.getQueueInfo(properties.worker().placeholderQueue()))
                .isNotNull();
    }
}
