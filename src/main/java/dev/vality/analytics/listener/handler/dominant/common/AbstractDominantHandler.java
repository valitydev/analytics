package dev.vality.analytics.listener.handler.dominant.common;

import dev.vality.damsel.domain.DomainObject;
import dev.vality.damsel.domain.Reference;
import dev.vality.damsel.domain_config_v2.FinalOperation;

import java.util.Optional;
import java.util.function.Function;

public abstract class AbstractDominantHandler<T> implements DominantHandler {

    private final Function<FinalOperation, Optional<T>> extractor;

    protected AbstractDominantHandler(Function<FinalOperation, Optional<T>> extractor) {
        this.extractor = extractor;
    }

    protected T extract(FinalOperation operation) {
        return extractor.apply(operation).orElseThrow(() -> new IllegalStateException("Object not found"));
    }

    protected boolean matches(FinalOperation operation, Function<T, Boolean> function) {
        return extractor.apply(operation).map(function).orElse(false);
    }

    public abstract static class SaveOrUpdateHandler extends AbstractDominantHandler<DomainObject> {
        protected SaveOrUpdateHandler() {
            super(operation -> {
                if (operation.isSetInsert()) {
                    return Optional.of(operation.getInsert().getObject());
                } else if (operation.isSetUpdate()) {
                    return Optional.of(operation.getUpdate().getObject());
                } else {
                    return Optional.empty();
                }
            });
        }
    }

    public abstract static class RemoveHandler extends AbstractDominantHandler<Reference> {
        protected RemoveHandler() {
            super(operation -> {
                if (operation.isSetRemove()) {
                    return Optional.of(operation.getRemove().getRef());
                } else {
                    return Optional.empty();
                }
            });
        }
    }
}
