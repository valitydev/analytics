package dev.vality.analytics.listener.handler.dominant.common;

import dev.vality.damsel.domain_config_v2.Author;
import dev.vality.damsel.domain_config_v2.FinalOperation;

public interface DominantHandler {

    void handle(FinalOperation operation, Author changedBy, long versionId);

    boolean isHandle(FinalOperation change);

}
