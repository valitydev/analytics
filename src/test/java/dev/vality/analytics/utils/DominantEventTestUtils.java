package dev.vality.analytics.utils;

import dev.vality.damsel.domain_config_v2.Author;
import dev.vality.damsel.domain_config_v2.FinalOperation;
import dev.vality.damsel.domain_config_v2.HistoricalCommit;

import java.util.List;

public class DominantEventTestUtils {

    public static final Author CHANGED_BY = new Author("test_id", "test_email", "test_name");

    public static HistoricalCommit create(FinalOperation finalOperation, long version) {
        return new HistoricalCommit()
                .setCreatedAt("2016-03-22T06:12:27Z")
                .setOps(List.of(finalOperation))
                .setChangedBy(CHANGED_BY)
                .setVersion(version);

    }
}
