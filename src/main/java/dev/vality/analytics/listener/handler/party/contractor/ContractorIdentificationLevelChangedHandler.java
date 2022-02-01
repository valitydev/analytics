package dev.vality.analytics.listener.handler.party.contractor;

import dev.vality.analytics.dao.repository.postgres.party.management.ContractorDao;
import dev.vality.analytics.domain.db.enums.ContractorIdentificationLvl;
import dev.vality.analytics.domain.db.tables.pojos.Contractor;
import dev.vality.analytics.listener.handler.merger.ContractorEventMerger;
import dev.vality.analytics.listener.handler.party.AbstractClaimChangeHandler;
import dev.vality.damsel.domain.ContractorIdentificationLevel;
import dev.vality.damsel.payment_processing.ClaimEffect;
import dev.vality.damsel.payment_processing.ContractorEffectUnit;
import dev.vality.damsel.payment_processing.PartyChange;
import dev.vality.geck.common.util.TypeUtil;
import dev.vality.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Component
@RequiredArgsConstructor
public class ContractorIdentificationLevelChangedHandler extends AbstractClaimChangeHandler {

    private final ContractorDao contractorDao;
    private final ContractorEventMerger contractorEventMerger;

    @Override
    public boolean accept(PartyChange change) {
        return isClaimEffect(change, claimEffect -> claimEffect.isSetContractorEffect()
                && claimEffect.getContractorEffect().getEffect().isSetIdentificationLevelChanged());
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void handleChange(PartyChange change, MachineEvent event) {
        getClaimStatus(change).getAccepted().getEffects().stream()
                .filter(claimEffect -> claimEffect.isSetContractorEffect()
                        && claimEffect.getContractorEffect().getEffect().isSetIdentificationLevelChanged())
                .forEach(claimEffect -> handleEvent(event, claimEffect));
    }

    private void handleEvent(MachineEvent event, ClaimEffect effect) {
        ContractorEffectUnit contractorEffect = effect.getContractorEffect();
        ContractorIdentificationLevel identificationLevelChanged =
                contractorEffect.getEffect().getIdentificationLevelChanged();
        String contractorId = contractorEffect.getId();
        String partyId = event.getSourceId();

        Contractor currentContractor = new Contractor();
        currentContractor.setPartyId(partyId);
        currentContractor.setEventId(event.getEventId());
        currentContractor.setEventTime(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));
        currentContractor.setContractorId(contractorId);
        currentContractor.setContractorIdentificationLevel(ContractorIdentificationLvl
                .valueOf(identificationLevelChanged.name())
        );

        log.debug("ContractorCreatedHandler result contractor: {}", currentContractor);

        final Contractor mergedContractor = contractorEventMerger.merge(contractorId, currentContractor);
        contractorDao.saveContractor(mergedContractor);
    }

}
