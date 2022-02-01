package dev.vality.analytics.listener.handler.party.shop;

import dev.vality.analytics.constant.EventType;
import dev.vality.analytics.dao.repository.postgres.party.management.ShopDao;
import dev.vality.analytics.domain.db.tables.pojos.Shop;
import dev.vality.analytics.listener.handler.ChangeHandler;
import dev.vality.analytics.listener.handler.merger.ShopEventMerger;
import dev.vality.damsel.domain.Suspension;
import dev.vality.damsel.payment_processing.PartyChange;
import dev.vality.geck.common.util.TypeUtil;
import dev.vality.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Component
@RequiredArgsConstructor
public class ShopSuspensionHandler implements ChangeHandler<PartyChange, MachineEvent> {

    private final ShopEventMerger shopEventMerger;
    private final ShopDao shopDao;

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void handleChange(PartyChange change, MachineEvent event) {
        String shopId = change.getShopSuspension().getShopId();
        String partyId = event.getSourceId();

        Shop shop = new Shop();
        shop.setPartyId(partyId);
        shop.setShopId(shopId);
        shop.setEventId(event.getEventId());
        shop.setEventTime(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));

        Suspension suspension = change.getShopSuspension().getSuspension();
        if (suspension.isSetActive()) {
            shop.setSuspension(dev.vality.analytics.domain.db.enums.Suspension.active);
            shop.setSuspensionActiveSince(TypeUtil.stringToLocalDateTime(suspension.getActive().getSince()));
        } else if (suspension.isSetSuspended()) {
            shop.setSuspension(dev.vality.analytics.domain.db.enums.Suspension.suspended);
            shop.setSuspensionSuspendedSince(TypeUtil.stringToLocalDateTime(suspension.getSuspended().getSince()));
        }

        final Shop mergedShop = shopEventMerger.mergeShop(partyId, shopId, shop);
        shopDao.saveShop(mergedShop);
    }

    @Override
    public EventType getChangeType() {
        return EventType.SHOP_SUSPENSION;
    }

}
