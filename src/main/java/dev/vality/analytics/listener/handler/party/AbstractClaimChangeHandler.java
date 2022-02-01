package dev.vality.analytics.listener.handler.party;

import dev.vality.analytics.constant.EventType;
import dev.vality.analytics.listener.handler.ChangeHandler;
import dev.vality.damsel.payment_processing.ClaimEffect;
import dev.vality.damsel.payment_processing.ClaimStatus;
import dev.vality.damsel.payment_processing.PartyChange;
import dev.vality.geck.filter.Filter;
import dev.vality.geck.filter.PathConditionFilter;
import dev.vality.geck.filter.condition.IsNullCondition;
import dev.vality.geck.filter.rule.PathConditionRule;
import dev.vality.machinegun.eventsink.MachineEvent;

import java.util.List;
import java.util.function.Predicate;

public abstract class AbstractClaimChangeHandler implements ChangeHandler<PartyChange, MachineEvent> {

    private static final Filter CLAIM_CREATED_FILTER = new PathConditionFilter(
            new PathConditionRule("claim_created.status.accepted", new IsNullCondition().not()));

    private static final Filter CLAIM_STATUS_CHANGED_FILTER = new PathConditionFilter(
            new PathConditionRule("claim_status_changed.status.accepted", new IsNullCondition().not()));

    @Override
    public boolean accept(PartyChange change) {
        return CLAIM_CREATED_FILTER.match(change) || CLAIM_STATUS_CHANGED_FILTER.match(change);
    }

    protected ClaimStatus getClaimStatus(PartyChange change) {
        ClaimStatus claimStatus = null;
        if (change.isSetClaimCreated()) {
            claimStatus = change.getClaimCreated().getStatus();
        } else if (change.isSetClaimStatusChanged()) {
            claimStatus = change.getClaimStatusChanged().getStatus();
        }
        return claimStatus;
    }

    @Override
    public EventType getChangeType() {
        return null;
    }

    protected boolean isClaimEffect(PartyChange change, Predicate<ClaimEffect> predicate) {
        if (CLAIM_CREATED_FILTER.match(change) || CLAIM_STATUS_CHANGED_FILTER.match(change)) {
            List<ClaimEffect> claimEffects = getClaimStatus(change).getAccepted().getEffects();
            return claimEffects.stream().anyMatch(predicate);
        }
        return false;
    }
}
