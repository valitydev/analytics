package dev.vality.analytics.listener.handler.dominant.common;

import dev.vality.damsel.domain_config_v2.FinalOperation;
import dev.vality.damsel.domain_config_v2.HistoricalCommit;

public interface DominantHandler {

    void handle(FinalOperation operation, HistoricalCommit historicalCommit);

    boolean isHandle(FinalOperation change);

}
