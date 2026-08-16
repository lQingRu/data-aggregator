package com.dataaggregator.workflow;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public record HybridChunkSearchWorkflow(int version, List<WorkflowStepDefinition> stepDefinitions) {

    public static final String WORKFLOW_ID = "hybrid_chunk_search";
    public static final String LEXICAL_RETRIEVAL = "lexical_retrieval";
    public static final String SEMANTIC_RETRIEVAL = "semantic_retrieval";
    public static final String MOCK_RELEVANCE_SCORING = "mock_relevance_scoring";
    public static final String SNAPSHOT_PROJECTION = "snapshot_projection";

    public HybridChunkSearchWorkflow {
        stepDefinitions = List.copyOf(stepDefinitions);
    }

    public static HybridChunkSearchWorkflow defaultWorkflow(int version) {
        return workflowWithQueues(
                version, "search.lexical", "search.semantic", "enrichment.relevance_score", "snapshot.projector");
    }

    public HybridChunkSearchWorkflow withQueues(
            String lexicalQueue, String semanticQueue, String relevanceScoreQueue, String snapshotProjectorQueue) {
        return workflowWithQueues(version, lexicalQueue, semanticQueue, relevanceScoreQueue, snapshotProjectorQueue);
    }

    private static HybridChunkSearchWorkflow workflowWithQueues(
            int version,
            String lexicalQueue,
            String semanticQueue,
            String relevanceScoreQueue,
            String snapshotProjectorQueue) {
        return new HybridChunkSearchWorkflow(
                version,
                List.of(
                        new WorkflowStepDefinition(LEXICAL_RETRIEVAL, lexicalQueue, true, true, 3, List.of()),
                        new WorkflowStepDefinition(SEMANTIC_RETRIEVAL, semanticQueue, false, true, 3, List.of()),
                        new WorkflowStepDefinition(
                                MOCK_RELEVANCE_SCORING,
                                relevanceScoreQueue,
                                false,
                                true,
                                3,
                                List.of(SEMANTIC_RETRIEVAL)),
                        new WorkflowStepDefinition(
                                SNAPSHOT_PROJECTION,
                                snapshotProjectorQueue,
                                true,
                                true,
                                3,
                                List.of(LEXICAL_RETRIEVAL, SEMANTIC_RETRIEVAL, MOCK_RELEVANCE_SCORING))));
    }

    public WorkflowStepDefinition lexicalRetrieval() {
        return step(LEXICAL_RETRIEVAL);
    }

    public WorkflowStepDefinition semanticRetrieval() {
        return step(SEMANTIC_RETRIEVAL);
    }

    public WorkflowStepDefinition mockRelevanceScoring() {
        return step(MOCK_RELEVANCE_SCORING);
    }

    public WorkflowStepDefinition snapshotProjection() {
        return step(SNAPSHOT_PROJECTION);
    }

    public WorkflowStepDefinition step(String stepId) {
        return stepsById().get(stepId);
    }

    public Map<String, WorkflowStepDefinition> stepsById() {
        return stepDefinitions.stream().collect(Collectors.toMap(WorkflowStepDefinition::id, Function.identity()));
    }
}
