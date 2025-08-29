package dev.vality.analytics.listener.handler.dominant;

import dev.vality.analytics.dao.repository.postgres.party.management.ShopDao;
import dev.vality.analytics.domain.db.tables.pojos.Shop;
import dev.vality.analytics.listener.handler.merger.ShopEventMerger;
import dev.vality.analytics.utils.TestData;
import dev.vality.damsel.domain.DomainObject;
import dev.vality.damsel.domain.ShopConfigObject;
import dev.vality.damsel.domain_config_v2.FinalInsertOp;
import dev.vality.damsel.domain_config_v2.FinalOperation;
import dev.vality.damsel.domain_config_v2.HistoricalCommit;
import dev.vality.damsel.domain_config_v2.UpdateOp;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ShopSaveOrUpdateHandlerTest {

    @Mock
    private ShopDao shopDao;

    @Mock
    private ShopEventMerger shopEventMerger;

    private ShopSaveOrUpdateHandler shopSaveOrUpdateHandler;
    private ShopConfigObject shopConfigObject;
    private HistoricalCommit historicalCommit;

    @BeforeEach
    void setUp() {
        shopConfigObject = TestData.buildShopConfigObject();
        historicalCommit = TestData.buildHistoricalCommit();
        shopSaveOrUpdateHandler = new ShopSaveOrUpdateHandler(shopDao, shopEventMerger);
    }

    @Test
    void shouldHandleInsertOp() {
        var operation = new FinalOperation();
        var insertOp = new FinalInsertOp();
        DomainObject domainObject = new DomainObject();
        domainObject.setShopConfig(shopConfigObject);
        insertOp.setObject(domainObject);
        operation.setInsert(insertOp);

        when(shopEventMerger.mergeShop(anyString(), anyString(), any(Shop.class))).thenReturn(new Shop());

        shopSaveOrUpdateHandler.handle(operation, historicalCommit);

        verify(shopDao, times(1)).saveShop(any(Shop.class));
        verify(shopEventMerger, times(1)).mergeShop(anyString(), anyString(), any(Shop.class));
    }

    @Test
    void shouldHandleUpdateOp() {
        var operation = new FinalOperation();
        var updateOp = new UpdateOp();
        DomainObject domainObject = new DomainObject();
        domainObject.setShopConfig(shopConfigObject);
        updateOp.setObject(domainObject);
        operation.setUpdate(updateOp);

        when(shopEventMerger.mergeShop(anyString(), anyString(), any(Shop.class))).thenReturn(new Shop());

        shopSaveOrUpdateHandler.handle(operation, historicalCommit);

        verify(shopDao, times(1)).saveShop(any(Shop.class));
        verify(shopEventMerger, times(1)).mergeShop(anyString(), anyString(), any(Shop.class));
    }

    @Test
    void successCheckHandle() {
        var operation = new FinalOperation();
        var insertOp = new FinalInsertOp();
        DomainObject domainObject = new DomainObject();
        domainObject.setShopConfig(shopConfigObject);
        insertOp.setObject(domainObject);
        operation.setInsert(insertOp);

        assertTrue(shopSaveOrUpdateHandler.isHandle(operation));
    }

    @Test
    void notSuccessCheckHandle() {
        var operation = new FinalOperation();
        var insertOp = new FinalInsertOp();
        DomainObject domainObject = new DomainObject();
        insertOp.setObject(domainObject);
        operation.setInsert(insertOp);

        assertFalse(shopSaveOrUpdateHandler.isHandle(operation));
    }


}
