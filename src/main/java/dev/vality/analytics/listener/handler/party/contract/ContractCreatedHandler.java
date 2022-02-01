package dev.vality.analytics.listener.handler.party.contract;

import dev.vality.analytics.converter.ContractorToCurrentContractorConverter;
import dev.vality.analytics.dao.repository.postgres.party.management.ContractDao;
import dev.vality.analytics.dao.repository.postgres.party.management.ContractorDao;
import dev.vality.analytics.domain.db.tables.pojos.Contract;
import dev.vality.analytics.listener.handler.party.AbstractClaimChangeHandler;
import dev.vality.damsel.domain.Contractor;
import dev.vality.damsel.payment_processing.ClaimEffect;
import dev.vality.damsel.payment_processing.ContractEffectUnit;
import dev.vality.damsel.payment_processing.PartyChange;
import dev.vality.geck.common.util.TypeUtil;
import dev.vality.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
@Component
@Order(3)
@RequiredArgsConstructor
public class ContractCreatedHandler extends AbstractClaimChangeHandler {

    private final ContractorToCurrentContractorConverter contractorToCurrentContractorConverter;
    private final ContractorDao contractorDao;
    private final ContractDao contractDao;

    @Override
    public boolean accept(PartyChange change) {
        return isClaimEffect(change, claimEffect -> claimEffect.isSetContractEffect()
                && claimEffect.getContractEffect().getEffect().isSetCreated());
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void handleChange(PartyChange change, MachineEvent event) {
        log.debug("Contract create change: {}", change);
        List<ClaimEffect> claimEffects = getClaimStatus(change).getAccepted().getEffects().stream()
                .filter(claimEffect -> claimEffect.isSetContractEffect()
                        && claimEffect.getContractEffect().getEffect().isSetCreated())
                .collect(Collectors.toList());
        for (ClaimEffect claimEffect : claimEffects) {
            handleEvent(event, claimEffect);
        }
        log.debug("Contract create finished");
    }

    private void handleEvent(MachineEvent event, ClaimEffect effect) {
        log.debug("Contract create handle: {}", effect);

        String partyId = event.getSourceId();
        ContractEffectUnit contractEffectUnit = effect.getContractEffect();
        dev.vality.damsel.domain.Contract contractCreated = contractEffectUnit.getEffect().getCreated();

        Contract contract = new Contract();
        contract.setPartyId(partyId);
        contract.setEventId(event.getEventId());
        contract.setEventTime(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));

        String contractorId = checkAndCreateContractor(event, partyId, contractCreated);
        contract.setContractorId(contractorId);
        contract.setContractId(contractEffectUnit.getContractId());
        contractDao.saveContract(contract);

        log.debug("Contract create result: {}", contract);
    }

    private String checkAndCreateContractor(MachineEvent event, String partyId,
                                            dev.vality.damsel.domain.Contract contractCreated) {
        Contractor contractor = contractCreated.getContractor();
        log.debug("ContractCreatedHandler contractor: {}", contractor);
        String contractorId = initContractorId(contractCreated);
        if (contractor != null) {
            dev.vality.analytics.domain.db.tables.pojos.Contractor currentContractor =
                    contractorToCurrentContractorConverter.convert(contractor);
            currentContractor.setContractorId(contractorId);
            currentContractor.setEventId(event.getEventId());
            currentContractor.setEventTime(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));
            currentContractor.setPartyId(partyId);
            contractorDao.saveContractor(currentContractor);
            log.debug("ContractCreatedHandler save currentContractor: {}", currentContractor);
        }
        return contractorId;
    }

    private String initContractorId(dev.vality.damsel.domain.Contract contractCreated) {
        String contractorId = "";
        if (contractCreated.isSetContractorId()) {
            contractorId = contractCreated.getContractorId();
        } else if (contractCreated.isSetContractor()) {
            contractorId = UUID.randomUUID().toString();
        }
        return contractorId;
    }

}
