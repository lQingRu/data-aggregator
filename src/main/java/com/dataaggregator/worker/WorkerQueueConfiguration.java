package com.dataaggregator.worker;

import com.dataaggregator.config.DataAggregatorProperties;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Configuration
@EnableRabbit
@Profile("worker")
public class WorkerQueueConfiguration {

    @Bean
    Queue placeholderWorkerQueue(DataAggregatorProperties properties) {
        return new Queue(properties.worker().placeholderQueue(), true);
    }
}
