package dev.vality.analytics.listener.handler.dominant;

import dev.vality.analytics.dao.repository.postgres.party.management.ShopDao;
import dev.vality.analytics.domain.db.tables.pojos.Shop;
import dev.vality.analytics.listener.handler.dominant.common.AbstractDominantHandler;
import dev.vality.damsel.domain.Reference;
import dev.vality.damsel.domain.ShopConfigRef;
import dev.vality.damsel.domain_config_v2.FinalOperation;
import dev.vality.damsel.domain_config_v2.HistoricalCommit;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Component
@RequiredArgsConstructor
public class ShopRemoveHandler extends AbstractDominantHandler.RemoveHandler {

    private final ShopDao shopDao;

    @Override
    @Transactional
    public void handle(FinalOperation operation, HistoricalCommit historicalCommit) {
        var reference = extract(operation);
        var shopRef = reference.getShopConfig();
        log.info(
                "Remove shopConfig operation. shopId='{}' version='{}'",
                shopRef.getId(), historicalCommit.getVersion()
        );

        var shop = convertToDatabaseObject(shopRef, historicalCommit);
        shopDao.removeShop(shop);
    }

    @Override
    public boolean isHandle(FinalOperation change) {
        return matches(change, Reference::isSetShopConfig);
    }

    private Shop convertToDatabaseObject(ShopConfigRef shopRef, HistoricalCommit historicalCommit) {
        var changedBy = historicalCommit.getChangedBy();
        var shop = new Shop();
        shop.setVersionId(historicalCommit.getVersion());
        shop.setShopId(shopRef.getId());
        shop.setChangedById(changedBy.getId());
        shop.setChangedByName(changedBy.getName());
        shop.setChangedByEmail(changedBy.getEmail());
        return shop;
    }
}
