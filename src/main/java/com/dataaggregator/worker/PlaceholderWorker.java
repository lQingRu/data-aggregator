package com.dataaggregator.worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile("worker")
public class PlaceholderWorker {

    private static final Logger log = LoggerFactory.getLogger(PlaceholderWorker.class);

    @RabbitListener(queues = "${data-aggregator.worker.placeholder-queue}")
    public void receivePlaceholderCommand(String payload) {
        log.info("Received placeholder worker command: {}", payload);
    }
}
