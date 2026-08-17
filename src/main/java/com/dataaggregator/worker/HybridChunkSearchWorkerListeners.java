package com.dataaggregator.worker;

import com.dataaggregator.workflow.WorkerCommand;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile("worker")
public class HybridChunkSearchWorkerListeners {

    private final HybridChunkSearchWorkers workers;

    public HybridChunkSearchWorkerListeners(HybridChunkSearchWorkers workers) {
        this.workers = workers;
    }

    @RabbitListener(id = "hybridChunkSearch.lexicalRetrieval", queues = "${data-aggregator.workflow.lexical-queue}")
    public void runLexicalRetrieval(WorkerCommand command) {
        workers.runLexicalRetrieval(command);
    }

    @RabbitListener(id = "hybridChunkSearch.semanticRetrieval", queues = "${data-aggregator.workflow.semantic-queue}")
    public void runSemanticRetrieval(WorkerCommand command) {
        workers.runSemanticRetrieval(command);
    }

    @RabbitListener(
            id = "hybridChunkSearch.mockRelevanceScoring",
            queues = "${data-aggregator.workflow.relevance-score-queue}")
    public void runMockRelevanceScoring(WorkerCommand command) {
        workers.runMockRelevanceScoring(command);
    }

    @RabbitListener(
            id = "hybridChunkSearch.snapshotProjection",
            queues = "${data-aggregator.workflow.snapshot-projector-queue}")
    public void runSnapshotProjection(WorkerCommand command) {
        workers.runSnapshotProjection(command);
    }
}
