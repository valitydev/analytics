package dev.vality.analytics.listener.handler.merger;

import dev.vality.analytics.dao.repository.postgres.party.management.ShopDao;
import dev.vality.analytics.domain.db.tables.pojos.Shop;
import jakarta.validation.constraints.NotNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class ShopEventMerger {

    private final ShopDao shopDao;

    public Shop mergeShop(String partyId, String shopId, Shop shop, Shop targetShop) {
        return mergeShops(partyId, shopId, shop, targetShop);
    }

    public Shop mergeShop(String partyId, String shopId, Shop shop) {
        Shop targetShop = shopDao.getShopByPartyIdAndShopId(partyId, shopId);
        return mergeShops(partyId, shopId, shop, targetShop);
    }

    @NotNull
    private Shop mergeShops(String partyId, String shopId, Shop shop, Shop targetShop) {
        if (targetShop == null) {
            targetShop = new Shop();
        }
        targetShop.setPartyId(partyId);
        targetShop.setShopId(shopId);
        targetShop.setEventId(shop.getEventId());
        targetShop.setEventTime(shop.getEventTime());
        targetShop.setCategoryId(shop.getCategoryId() != null ? shop.getCategoryId() : targetShop.getCategoryId());
        targetShop.setCreatedAt(shop.getCreatedAt() != null ? shop.getCreatedAt() : targetShop.getCreatedAt());
        targetShop.setBlocking(shop.getBlocking() != null ? shop.getBlocking() : targetShop.getBlocking());
        targetShop.setBlockedReason(shop.getBlockedReason() != null
                ? shop.getBlockedReason() : targetShop.getBlockedReason());
        targetShop.setBlockedSince(shop.getBlockedSince() != null
                ? shop.getBlockedSince() : targetShop.getBlockedSince());
        targetShop.setUnblockedReason(shop.getUnblockedReason() != null
                ? shop.getUnblockedReason() : targetShop.getUnblockedReason());
        targetShop.setUnblockedSince(shop.getUnblockedSince() != null
                ? shop.getUnblockedSince() : targetShop.getUnblockedSince());
        targetShop.setSuspension(shop.getSuspension() != null ? shop.getSuspension() : targetShop.getSuspension());
        targetShop.setSuspensionActiveSince(shop.getSuspensionActiveSince() != null
                ? shop.getSuspensionActiveSince() : targetShop.getSuspensionActiveSince());
        targetShop.setSuspensionSuspendedSince(shop.getSuspensionSuspendedSince() != null
                ? shop.getSuspensionSuspendedSince() : targetShop.getSuspensionSuspendedSince());
        targetShop.setDetailsName(shop.getDetailsName() != null
                ? shop.getDetailsName() : targetShop.getDetailsName());
        targetShop.setDetailsDescription(shop.getDetailsDescription() != null
                ? shop.getDetailsDescription() : targetShop.getDetailsDescription());
        targetShop.setLocationUrl(shop.getLocationUrl() != null ? shop.getLocationUrl() : targetShop.getLocationUrl());
        targetShop.setAccountCurrencyCode(shop.getAccountCurrencyCode() != null
                ? shop.getAccountCurrencyCode() : targetShop.getAccountCurrencyCode());
        targetShop.setAccountSettlement(shop.getAccountSettlement() != null
                ? shop.getAccountSettlement() : targetShop.getAccountSettlement());
        targetShop.setAccountGuarantee(shop.getAccountGuarantee() != null
                ? shop.getAccountGuarantee() : targetShop.getAccountGuarantee());

        log.debug("ShopEventMerger target shop: {}", targetShop);
        return targetShop;
    }
}
