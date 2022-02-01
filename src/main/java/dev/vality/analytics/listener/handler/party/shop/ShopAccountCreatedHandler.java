package dev.vality.analytics.listener.handler.party.shop;

import dev.vality.analytics.dao.repository.postgres.party.management.ShopDao;
import dev.vality.analytics.domain.db.tables.pojos.Shop;
import dev.vality.analytics.listener.handler.merger.ShopEventMerger;
import dev.vality.analytics.listener.handler.party.AbstractClaimChangeHandler;
import dev.vality.damsel.domain.ShopAccount;
import dev.vality.damsel.payment_processing.ClaimEffect;
import dev.vality.damsel.payment_processing.PartyChange;
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
public class ShopAccountCreatedHandler extends AbstractClaimChangeHandler {

    private final ShopEventMerger shopEventMerger;
    private final ShopDao shopDao;

    @Override
    public boolean accept(PartyChange change) {
        return isClaimEffect(change, claimEffect -> claimEffect.isSetShopEffect()
                && claimEffect.getShopEffect().getEffect().isSetAccountCreated());

    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void handleChange(PartyChange change, MachineEvent event) {
        List<ClaimEffect> claimEffects = getClaimStatus(change).getAccepted().getEffects();
        claimEffects.stream()
                .filter(claimEffect -> claimEffect.isSetShopEffect()
                        && claimEffect.getShopEffect().getEffect().isSetAccountCreated())
                .forEach(claimEffect -> handleEvent(event, claimEffect));
    }

    private void handleEvent(MachineEvent event, ClaimEffect effect) {
        ShopEffectUnit shopEffect = effect.getShopEffect();
        ShopAccount accountCreated = shopEffect.getEffect().getAccountCreated();
        String shopId = shopEffect.getShopId();
        String partyId = event.getSourceId();

        Shop shop = new Shop();
        shop.setPartyId(partyId);
        shop.setShopId(shopId);
        shop.setEventId(event.getEventId());
        shop.setEventTime(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));
        shop.setAccountCurrencyCode(accountCreated.getCurrency().getSymbolicCode());
        shop.setAccountGuarantee(String.valueOf(accountCreated.getGuarantee()));
        shop.setAccountSettlement(String.valueOf(accountCreated.getSettlement()));
        shop.setAccountPayout(String.valueOf(accountCreated.getPayout()));

        final Shop mergedShop = shopEventMerger.mergeShop(partyId, shopId, shop);
        shopDao.saveShop(mergedShop);
    }

}
