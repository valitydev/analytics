package dev.vality.analytics.listener.handler.party.shop;

import dev.vality.analytics.converter.ContractorToShopConverter;
import dev.vality.analytics.dao.repository.postgres.party.management.ContractDao;
import dev.vality.analytics.dao.repository.postgres.party.management.ContractorDao;
import dev.vality.analytics.dao.repository.postgres.party.management.ShopDao;
import dev.vality.analytics.domain.db.tables.pojos.Contract;
import dev.vality.analytics.domain.db.tables.pojos.Contractor;
import dev.vality.analytics.domain.db.tables.pojos.Shop;
import dev.vality.analytics.listener.handler.merger.ShopEventMerger;
import dev.vality.analytics.listener.handler.party.AbstractClaimChangeHandler;
import dev.vality.damsel.payment_processing.ClaimEffect;
import dev.vality.damsel.payment_processing.PartyChange;
import dev.vality.damsel.payment_processing.ShopContractChanged;
import dev.vality.damsel.payment_processing.ShopEffectUnit;
import dev.vality.geck.common.util.TypeUtil;
import dev.vality.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Component
@RequiredArgsConstructor
public class ShopContractChangedHandler extends AbstractClaimChangeHandler {

    private final ContractorToShopConverter contractorToShopConverter;
    private final ShopEventMerger shopEventMerger;
    private final ShopDao shopDao;
    private final ContractDao contractDao;
    private final ContractorDao contractorDao;

    @Override
    public boolean accept(PartyChange change) {
        return isClaimEffect(change, claimEffect -> claimEffect.isSetShopEffect()
                && claimEffect.getShopEffect().getEffect().isSetContractChanged());
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void handleChange(PartyChange change, MachineEvent event) {
        List<ClaimEffect> claimEffects = getClaimStatus(change).getAccepted().getEffects();
        claimEffects.stream()
                .filter(claimEffect -> claimEffect.isSetShopEffect()
                        && claimEffect.getShopEffect().getEffect().isSetContractChanged())
                .forEach(claimEffect -> handleEvent(event, claimEffect));
    }

    private void handleEvent(MachineEvent event, ClaimEffect effect) {
        ShopEffectUnit shopEffect = effect.getShopEffect();
        ShopContractChanged contractChanged = shopEffect.getEffect().getContractChanged();
        String shopId = shopEffect.getShopId();
        String partyId = event.getSourceId();
        final String contractId = contractChanged.getContractId();
        final Contract contract = contractDao.getContractByPartyIdAndContractId(partyId, contractId);
        final Contractor contractor = contractorDao.getContractorByPartyIdAndContractorId(partyId,
                contract.getContractorId()
        );

        Shop shop = contractorToShopConverter.convert(contractor);
        shop.setPartyId(partyId);
        shop.setShopId(shopId);
        shop.setEventId(event.getEventId());
        shop.setEventTime(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));
        shop.setContractId(contractChanged.getContractId());
        shop.setPayoutToolId(contractChanged.getPayoutToolId());

        final Shop mergedShop = shopEventMerger.mergeShop(partyId, shopId, shop);
        shopDao.saveShop(mergedShop);
    }

}
