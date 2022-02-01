package dev.vality.analytics.listener.handler.dominant;

import dev.vality.damsel.domain_config.Operation;

public interface DominantHandler {

    void handle(Operation operation, long versionId);

    boolean isHandle(Operation change);

}
