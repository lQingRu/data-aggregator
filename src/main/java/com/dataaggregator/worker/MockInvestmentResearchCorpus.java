package com.dataaggregator.worker;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.springframework.stereotype.Component;

@Component
public class MockInvestmentResearchCorpus {

    private static final List<Company> COMPANIES = List.of(
            new Company("V", "Visa Inc.", "financials"),
            new Company("MA", "Mastercard Incorporated", "financials"),
            new Company("NVDA", "NVIDIA Corporation", "technology"),
            new Company("TSM", "Taiwan Semiconductor Manufacturing Company", "technology"),
            new Company("XOM", "Exxon Mobil Corporation", "energy"),
            new Company("AAPL", "Apple Inc.", "consumer"),
            new Company("MSFT", "Microsoft Corporation", "technology"),
            new Company("UNH", "UnitedHealth Group Incorporated", "healthcare"));

    private static final List<String> REGIONS = List.of("Global", "North America", "Europe", "APAC", "Latin America");

    private static final List<DocumentKind> DOCUMENT_KINDS = List.of(
            new DocumentKind("report", "Internal Research"),
            new DocumentKind("memo", "Partner Feed"),
            new DocumentKind("transcript", "Earnings Transcript"),
            new DocumentKind("filing", "Public Filing"));

    private static final Map<String, String> AUTHORS = Map.of(
            "report", "Research Desk",
            "memo", "Strategy Desk",
            "transcript", "Earnings Desk",
            "filing", "Disclosure Team");

    private final List<InvestmentResearchChunk> chunks;

    public MockInvestmentResearchCorpus() {
        this.chunks = buildChunks();
    }

    public List<InvestmentResearchChunk> chunks() {
        return chunks;
    }

    public Optional<InvestmentResearchChunk> findByChunkId(String chunkId) {
        return chunks.stream().filter(chunk -> chunk.chunkId().equals(chunkId)).findFirst();
    }

    private static List<InvestmentResearchChunk> buildChunks() {
        List<InvestmentResearchChunk> generated = new ArrayList<>();
        int documentNumber = 1;
        int chunkNumber = 1;
        for (Company company : COMPANIES) {
            for (String region : REGIONS) {
                for (DocumentKind documentKind : DOCUMENT_KINDS) {
                    String parentEntityId = "doc_%04d".formatted(documentNumber);
                    String parentTitle = parentTitle(company, region, documentKind);
                    Instant publishedAt = publishedAt(documentNumber);
                    for (int chunkIndex = 0; chunkIndex < 2; chunkIndex++) {
                        generated.add(new InvestmentResearchChunk(
                                "chunk_%06d".formatted(chunkNumber),
                                parentEntityId,
                                parentTitle,
                                documentKind.parentType(),
                                documentKind.sourceName(),
                                company.ticker(),
                                company.companyName(),
                                company.sector(),
                                region,
                                publishedAt,
                                AUTHORS.get(documentKind.parentType()),
                                chunkIndex,
                                chunkText(company, region, documentKind, chunkIndex)));
                        chunkNumber++;
                    }
                    documentNumber++;
                }
            }
        }
        return List.copyOf(generated);
    }

    private static String parentTitle(Company company, String region, DocumentKind documentKind) {
        return "%s %s %s 2026".formatted(region, company.companyName(), titleNoun(documentKind.parentType()));
    }

    private static String titleNoun(String parentType) {
        return switch (parentType) {
            case "memo" -> "Strategy Memo";
            case "transcript" -> "Earnings Transcript";
            case "filing" -> "Regulatory Filing";
            default -> "Market Outlook";
        };
    }

    private static Instant publishedAt(int documentNumber) {
        int year = documentNumber % 2 == 0 ? 2026 : 2025;
        int month = (documentNumber % 12) + 1;
        int day = (documentNumber % 24) + 1;
        return LocalDate.of(year, month, day).atStartOfDay().toInstant(ZoneOffset.UTC);
    }

    private static String chunkText(Company company, String region, DocumentKind documentKind, int chunkIndex) {
        String regionalContext = "The %s view uses %s channel checks and management commentary."
                .formatted(region, documentKind.sourceName());
        if ("financials".equals(company.sector())) {
            if (chunkIndex == 0) {
                return "Digital wallet adoption accelerated across %s as payment growth improved for %s. %s"
                        .formatted(region, company.companyName(), regionalContext);
            }
            return ("%s reported merchant acceptance gains, cross-border card volume recovery, and mobile checkout "
                            + "usage that supports payment network revenue.")
                    .formatted(company.ticker());
        }
        if ("technology".equals(company.sector())) {
            if (chunkIndex == 0) {
                return "AI infrastructure spending and cloud workload migration lifted demand for %s in %s. %s"
                        .formatted(company.companyName(), region, regionalContext);
            }
            return ("%s supply constraints, data center capacity, and enterprise software renewals shaped the "
                            + "technology margin outlook.")
                    .formatted(company.ticker());
        }
        if ("energy".equals(company.sector())) {
            if (chunkIndex == 0) {
                return "Refining margins, liquefied natural gas demand, and upstream production discipline drove "
                        + "%s estimates in %s. %s".formatted(company.companyName(), region, regionalContext);
            }
            return ("%s capital allocation favored resilient cash flow, buybacks, and low-cost barrels across the "
                            + "energy cycle.")
                    .formatted(company.ticker());
        }
        if ("healthcare".equals(company.sector())) {
            if (chunkIndex == 0) {
                return "Managed care enrollment, medical cost trends, and pharmacy services influenced %s in %s. %s"
                        .formatted(company.companyName(), region, regionalContext);
            }
            return ("%s tracked reimbursement rates, utilization, and care delivery efficiency for healthcare "
                            + "earnings visibility.")
                    .formatted(company.ticker());
        }
        if (chunkIndex == 0) {
            return "Device replacement cycles, services attach rates, and consumer demand informed %s in %s. %s"
                    .formatted(company.companyName(), region, regionalContext);
        }
        return "%s monitored premium product mix, digital services, and retail channel inventory for consumer growth."
                .formatted(company.ticker());
    }

    private record Company(String ticker, String companyName, String sector) {}

    private record DocumentKind(String parentType, String sourceName) {}
}
