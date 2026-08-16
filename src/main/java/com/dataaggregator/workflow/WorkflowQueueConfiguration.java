package com.dataaggregator.workflow;

import com.dataaggregator.config.DataAggregatorProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class WorkflowQueueConfiguration {

    @Bean
    MessageConverter workflowMessageConverter(ObjectMapper objectMapper) {
        return new Jackson2JsonMessageConverter(objectMapper);
    }

    @Bean
    Queue lexicalRetrievalQueue(DataAggregatorProperties properties) {
        return new Queue(properties.workflow().lexicalQueue(), true);
    }

    @Bean
    Queue semanticRetrievalQueue(DataAggregatorProperties properties) {
        return new Queue(properties.workflow().semanticQueue(), true);
    }

    @Bean
    Queue relevanceScoreQueue(DataAggregatorProperties properties) {
        return new Queue(properties.workflow().relevanceScoreQueue(), true);
    }

    @Bean
    Queue snapshotProjectorQueue(DataAggregatorProperties properties) {
        return new Queue(properties.workflow().snapshotProjectorQueue(), true);
    }

    @Bean
    Queue workerCompletionEventQueue(DataAggregatorProperties properties) {
        return new Queue(properties.workflow().completionEventQueue(), true);
    }
}
