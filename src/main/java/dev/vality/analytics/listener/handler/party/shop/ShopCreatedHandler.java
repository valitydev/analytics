package dev.vality.analytics.listener.handler.party.shop;

import dev.vality.analytics.converter.ContractorToShopConverter;
import dev.vality.analytics.dao.repository.postgres.party.management.ContractDao;
import dev.vality.analytics.dao.repository.postgres.party.management.ContractorDao;
import dev.vality.analytics.dao.repository.postgres.party.management.ShopDao;
import dev.vality.analytics.domain.db.enums.Blocking;
import dev.vality.analytics.domain.db.enums.Suspension;
import dev.vality.analytics.domain.db.tables.pojos.Contract;
import dev.vality.analytics.domain.db.tables.pojos.Contractor;
import dev.vality.analytics.domain.db.tables.pojos.Shop;
import dev.vality.analytics.listener.handler.party.AbstractClaimChangeHandler;
import dev.vality.damsel.payment_processing.ClaimEffect;
import dev.vality.damsel.payment_processing.PartyChange;
import dev.vality.damsel.payment_processing.ShopEffectUnit;
import dev.vality.geck.common.util.TBaseUtil;
import dev.vality.geck.common.util.TypeUtil;
import dev.vality.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Slf4j
@Order(4)
@Component
@RequiredArgsConstructor
public class ShopCreatedHandler extends AbstractClaimChangeHandler {

    private final ShopDao shopDao;
    private final ContractDao contractDao;
    private final ContractorDao contractorDao;
    private final ContractorToShopConverter contractorToShopConverter;

    @Override
    public boolean accept(PartyChange change) {
        return isClaimEffect(change, claimEffect -> claimEffect.isSetShopEffect()
                && claimEffect.getShopEffect().getEffect().isSetCreated());
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void handleChange(PartyChange change, MachineEvent event) {
        log.debug("ShopCreatedHandler handleChange change: {}", change);
        List<ClaimEffect> claimEffects = getClaimStatus(change).getAccepted().getEffects();
        claimEffects.stream()
                .filter(claimEffect -> claimEffect.isSetShopEffect()
                        && claimEffect.getShopEffect().getEffect().isSetCreated())
                .forEach(claimEffect -> handleEvent(event, claimEffect));
    }

    private void handleEvent(MachineEvent event, ClaimEffect effect) {
        ShopEffectUnit shopEffect = effect.getShopEffect();
        dev.vality.damsel.domain.Shop shopCreated = shopEffect.getEffect().getCreated();
        String shopId = shopEffect.getShopId();
        String partyId = event.getSourceId();

        final String contractId = shopCreated.getContractId();
        final Contract contract = contractDao.getContractByPartyIdAndContractId(partyId, contractId);
        final Contractor currentContractor = contractorDao.getContractorByPartyIdAndContractorId(partyId,
                contract.getContractorId()
        );

        Shop shop = initShop(event, shopCreated, shopId, partyId, contractId, currentContractor);

        shopDao.saveShop(shop);
    }

    @NotNull
    private Shop initShop(MachineEvent event,
                          dev.vality.damsel.domain.Shop shopCreated,
                          String shopId,
                          String partyId,
                          String contractId,
                          Contractor currentContractor) {
        Shop shop = contractorToShopConverter.convert(currentContractor);
        shop.setEventId(event.getEventId());
        shop.setEventTime(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));
        shop.setShopId(shopId);
        shop.setPartyId(partyId);
        shop.setCreatedAt(TypeUtil.stringToLocalDateTime(shopCreated.getCreatedAt()));
        shop.setBlocking(TBaseUtil.unionFieldToEnum(shopCreated.getBlocking(), Blocking.class));
        if (shopCreated.getBlocking().isSetUnblocked()) {
            shop.setUnblockedReason(shopCreated.getBlocking().getUnblocked().getReason());
            shop.setUnblockedSince(TypeUtil.stringToLocalDateTime(shopCreated.getBlocking().getUnblocked().getSince()));
        } else if (shopCreated.getBlocking().isSetBlocked()) {
            shop.setBlockedReason(shopCreated.getBlocking().getBlocked().getReason());
            shop.setBlockedSince(TypeUtil.stringToLocalDateTime(shopCreated.getBlocking().getBlocked().getSince()));
        }
        shop.setSuspension(TBaseUtil.unionFieldToEnum(shopCreated.getSuspension(), Suspension.class));
        if (shopCreated.getSuspension().isSetActive()) {
            shop.setSuspensionActiveSince(TypeUtil.stringToLocalDateTime(
                    shopCreated.getSuspension().getActive().getSince())
            );
        } else if (shopCreated.getSuspension().isSetSuspended()) {
            shop.setSuspensionSuspendedSince(TypeUtil.stringToLocalDateTime(
                    shopCreated.getSuspension().getSuspended().getSince())
            );
        }
        shop.setDetailsName(shopCreated.getDetails().getName());
        shop.setDetailsDescription(shopCreated.getDetails().getDescription());
        if (shopCreated.getLocation().isSetUrl()) {
            shop.setLocationUrl(shopCreated.getLocation().getUrl());
        }
        shop.setCategoryId(shopCreated.getCategory().getId());
        if (shopCreated.isSetAccount()) {
            shop.setAccountCurrencyCode(shopCreated.getAccount().getCurrency().getSymbolicCode());
            shop.setAccountGuarantee(String.valueOf(shopCreated.getAccount().getGuarantee()));
            shop.setAccountSettlement(String.valueOf(shopCreated.getAccount().getSettlement()));
        }
        shop.setContractId(contractId);
        return shop;
    }


}
