package dev.vality.analytics.listener.handler.dominant;

import dev.vality.damsel.domain_config.Operation;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractDominantHandler implements DominantHandler {

    protected dev.vality.damsel.domain.DomainObject getDominantObject(Operation operation) {
        if (operation.isSetInsert()) {
            return operation.getInsert().getObject();
        } else if (operation.isSetUpdate()) {
            return operation.getUpdate().getNewObject();
        } else if (operation.isSetRemove()) {
            return operation.getRemove().getObject();
        } else {
            throw new IllegalStateException("Unknown operation type: " + operation);
        }
    }

}
