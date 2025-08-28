package dev.vality.analytics.listener.handler.dominant;

import dev.vality.analytics.dao.repository.postgres.party.management.ShopDao;
import dev.vality.analytics.domain.db.enums.Blocking;
import dev.vality.analytics.domain.db.enums.Suspension;
import dev.vality.analytics.domain.db.tables.pojos.Shop;
import dev.vality.analytics.listener.handler.dominant.common.AbstractDominantHandler;
import dev.vality.analytics.listener.handler.merger.ShopEventMerger;
import dev.vality.damsel.domain.DomainObject;
import dev.vality.damsel.domain.ShopConfigObject;
import dev.vality.damsel.domain_config_v2.FinalOperation;
import dev.vality.damsel.domain_config_v2.HistoricalCommit;
import dev.vality.geck.common.util.TBaseUtil;
import dev.vality.geck.common.util.TypeUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

@Slf4j
@Component
@RequiredArgsConstructor
public class ShopSaveOrUpdateHandler extends AbstractDominantHandler.SaveOrUpdateHandler {

    private final ShopDao shopDao;
    private final ShopEventMerger shopEventMerger;

    @Override
    @Transactional
    public void handle(FinalOperation operation, HistoricalCommit historicalCommit) {
        var shopConfigObject = extract(operation).getShopConfig();
        var newShopData = convertToDatabaseObject(shopConfigObject, historicalCommit);
        var mergedShop = shopEventMerger.mergeShop(
                shopConfigObject.getData().getPartyRef().getId(),
                shopConfigObject.getRef().getId(),
                newShopData
        );
        if (operation.isSetInsert()) {
            log.info(
                    "Save shopConfigObject operation. shopId='{}' version='{}'",
                    shopConfigObject.getRef().getId(), historicalCommit.getVersion()
            );
            mergedShop.setCreatedAt(TypeUtil.stringToLocalDateTime(historicalCommit.getCreatedAt()));
            mergedShop.setDeleted(false);
            shopDao.saveShop(mergedShop);
        } else if (operation.isSetUpdate()) {
            log.info(
                    "Update shopConfigObject operation. shopId='{}' version='{}'",
                    shopConfigObject.getRef().getId(), historicalCommit.getVersion()
            );
            shopDao.saveShop(mergedShop);
        }
    }

    @Override
    public boolean isHandle(FinalOperation change) {
        return matches(change, DomainObject::isSetShopConfig);
    }

    private Shop convertToDatabaseObject(ShopConfigObject shopConfigObject, HistoricalCommit historicalCommit) {
        var shopConfig = shopConfigObject.getData();
        LocalDateTime eventTime = TypeUtil.stringToLocalDateTime(historicalCommit.getCreatedAt());

        Shop shop = new Shop();
        shop.setPartyId(shopConfig.getPartyRef().getId());
        shop.setShopId(shopConfigObject.getRef().getId());
        shop.setVersionId(historicalCommit.getVersion());
        shop.setEventId(historicalCommit.getVersion());
        shop.setEventTime(eventTime);


        shop.setDetailsName(shopConfig.getName());
        shop.setDetailsDescription(shopConfig.getDescription());

        if (shopConfig.isSetLocation()) {
            shop.setLocationUrl(shopConfig.getLocation().getUrl());
        }

        if (shopConfig.isSetCategory()) {
            shop.setCategoryId(shopConfig.getCategory().getId());
        }

        if (shopConfig.isSetAccount()) {
            var accountCreated = shopConfig.getAccount();
            shop.setAccountCurrencyCode(accountCreated.getCurrency().getSymbolicCode());
            shop.setAccountGuarantee(String.valueOf(accountCreated.getGuarantee()));
            shop.setAccountSettlement(String.valueOf(accountCreated.getSettlement()));
        }

        if (shopConfig.isSetBlock()) {
            var blocking = shopConfig.getBlock();
            shop.setBlocking(TBaseUtil.unionFieldToEnum(blocking, Blocking.class));
            if (blocking.isSetBlocked()) {
                shop.setBlockedReason(blocking.getBlocked().getReason());
                shop.setBlockedSince(TypeUtil.stringToLocalDateTime(blocking.getBlocked().getSince()));
            } else if (blocking.isSetUnblocked()) {
                shop.setUnblockedReason(blocking.getUnblocked().getReason());
                shop.setUnblockedSince(TypeUtil.stringToLocalDateTime(blocking.getUnblocked().getSince()));
            }
        }

        if (shopConfig.isSetSuspension()) {
            var suspension = shopConfig.getSuspension();
            if (suspension.isSetActive()) {
                shop.setSuspension(Suspension.active);
                shop.setSuspensionActiveSince(TypeUtil.stringToLocalDateTime(suspension.getActive().getSince()));
            } else if (suspension.isSetSuspended()) {
                shop.setSuspension(Suspension.suspended);
                shop.setSuspensionSuspendedSince(TypeUtil.stringToLocalDateTime(suspension.getSuspended().getSince()));
            }
        }

        var changedBy = historicalCommit.getChangedBy();
        shop.setChangedById(changedBy.getId());
        shop.setChangedByName(changedBy.getName());
        shop.setChangedByEmail(changedBy.getEmail());

        return shop;
    }
}
