package com.dataaggregator.worker;

import com.dataaggregator.config.DataAggregatorProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Configuration
@Profile("worker")
public class WorkerStartupLogger {

    private static final Logger log = LoggerFactory.getLogger(WorkerStartupLogger.class);

    @Bean
    ApplicationRunner logWorkerStartup(DataAggregatorProperties properties) {
        return args -> log.info(
                "Worker process started for placeholder queue '{}'",
                properties.worker().placeholderQueue());
    }
}
