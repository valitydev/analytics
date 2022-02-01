package dev.vality.analytics.listener.handler.party.contract;

import dev.vality.analytics.converter.ContractorToShopConverter;
import dev.vality.analytics.dao.repository.postgres.party.management.ContractDao;
import dev.vality.analytics.dao.repository.postgres.party.management.ContractorDao;
import dev.vality.analytics.dao.repository.postgres.party.management.ShopDao;
import dev.vality.analytics.domain.db.tables.pojos.Contract;
import dev.vality.analytics.domain.db.tables.pojos.Contractor;
import dev.vality.analytics.domain.db.tables.pojos.Shop;
import dev.vality.analytics.listener.handler.merger.ContractMerger;
import dev.vality.analytics.listener.handler.merger.ShopEventMerger;
import dev.vality.analytics.listener.handler.party.AbstractClaimChangeHandler;
import dev.vality.damsel.payment_processing.ClaimEffect;
import dev.vality.damsel.payment_processing.ContractEffectUnit;
import dev.vality.damsel.payment_processing.PartyChange;
import dev.vality.geck.common.util.TypeUtil;
import dev.vality.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.Nullable;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@RequiredArgsConstructor
@Component
public class ContractContractorIDChangedHandler extends AbstractClaimChangeHandler {

    private final ContractDao contractDao;
    private final ContractorDao contractorDao;
    private final ShopDao shopDao;
    private final ContractMerger contractMerger;
    private final ShopEventMerger shopEventMerger;
    private final ContractorToShopConverter contractorToShopConverter;

    @Override
    public boolean accept(PartyChange change) {
        return isClaimEffect(change, claimEffect -> claimEffect.isSetContractEffect()
                && claimEffect.getContractEffect().getEffect().isSetContractorChanged());
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void handleChange(PartyChange change, MachineEvent event) {
        List<ClaimEffect> claimEffects = getClaimStatus(change).getAccepted().getEffects().stream()
                .filter(claimEffect -> claimEffect.isSetContractEffect()
                        && claimEffect.getContractEffect().getEffect().isSetContractorChanged())
                .collect(Collectors.toList());
        for (ClaimEffect claimEffect : claimEffects) {
            handleEvent(event, claimEffect);
        }
    }

    private void handleEvent(MachineEvent event, ClaimEffect effect) {
        ContractEffectUnit contractEffectUnit = effect.getContractEffect();
        String partyId = event.getSourceId();

        final Contract contract = new Contract();
        contract.setPartyId(partyId);
        contract.setContractorId(contractEffectUnit.getEffect().getContractorChanged());
        contract.setEventId(event.getEventId());
        contract.setEventTime(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));

        final Contract mergedContract = contractMerger.merge(contractEffectUnit.getContractId(), contract);
        contractDao.saveContract(mergedContract);
        updateShops(event, partyId, contract, mergedContract);
    }

    @Nullable
    private List<Shop> updateShops(MachineEvent event, String partyId, Contract contract, Contract mergedContract) {
        log.debug("Update shops with partyId: {} and contract: {}", partyId, contract);
        List<Shop> currentShopStates = shopDao.getShopsByPartyIdAndContractId(partyId, mergedContract.getContractId());
        log.debug("Update shops: {}", currentShopStates);
        if (currentShopStates != null) {
            Contractor currentContractorState = contractorDao.getContractorByPartyIdAndContractorId(partyId,
                    contract.getContractorId());
            Shop newShop = contractorToShopConverter.convert(currentContractorState);
            List<Shop> batch = currentShopStates.stream()
                    .map(shop -> map(shop, event, partyId, newShop))
                    .collect(Collectors.toList());
            shopDao.saveShop(batch);
            log.debug("Shops saved: {}", currentShopStates);
        }
        return currentShopStates;
    }

    private Shop map(Shop currentShopState, MachineEvent event, String partyId, Shop newShop) {
        Shop mergedShop = shopEventMerger.mergeShop(partyId, currentShopState.getShopId(), newShop, currentShopState);
        mergedShop.setEventId(event.getEventId());
        mergedShop.setEventTime(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));
        return mergedShop;
    }

}
