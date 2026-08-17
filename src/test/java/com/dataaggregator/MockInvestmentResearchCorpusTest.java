package com.dataaggregator;

import static org.assertj.core.api.Assertions.assertThat;

import com.dataaggregator.worker.MockInvestmentResearchCorpus;
import org.junit.jupiter.api.Test;

class MockInvestmentResearchCorpusTest {

    @Test
    void corpusContainsStablePhaseOneInvestmentResearchChunks() {
        MockInvestmentResearchCorpus corpus = new MockInvestmentResearchCorpus();

        assertThat(corpus.chunks()).hasSize(320);
        assertThat(corpus.chunks().getFirst().chunkId()).isEqualTo("chunk_000001");
        assertThat(corpus.chunks().getFirst().parentEntityId()).isEqualTo("doc_0001");
        assertThat(corpus.chunks().getLast().chunkId()).isEqualTo("chunk_000320");
        assertThat(corpus.chunks())
                .extracting("ticker")
                .contains("V", "MA", "NVDA", "TSM", "XOM", "AAPL", "MSFT", "UNH");
        assertThat(corpus.chunks()).extracting("parentType").contains("report", "memo", "transcript", "filing");
    }
}
