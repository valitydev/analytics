package dev.vality.analytics.listener.handler.dominant;

import dev.vality.analytics.dao.repository.postgres.party.management.ShopDao;
import dev.vality.analytics.domain.db.tables.pojos.Shop;
import dev.vality.analytics.utils.TestData;
import dev.vality.damsel.domain.Reference;
import dev.vality.damsel.domain.ShopConfigRef;
import dev.vality.damsel.domain_config_v2.FinalOperation;
import dev.vality.damsel.domain_config_v2.HistoricalCommit;
import dev.vality.damsel.domain_config_v2.RemoveOp;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class ShopRemoveHandlerTest {

    @Mock
    private ShopDao shopDao;

    private ShopRemoveHandler shopRemoveHandler;
    private HistoricalCommit historicalCommit;

    @BeforeEach
    void setUp() {
        historicalCommit = TestData.buildHistoricalCommit();
        shopRemoveHandler = new ShopRemoveHandler(shopDao);
    }

    @Test
    void shouldHandleRemoveOp() {
        Reference reference = new Reference();
        ShopConfigRef shopConfigRef = new ShopConfigRef();
        shopConfigRef.setId("test-shop-id");
        reference.setShopConfig(shopConfigRef);

        var operation = new FinalOperation();
        var removeOp = new RemoveOp();
        removeOp.setRef(reference);
        operation.setRemove(removeOp);

        shopRemoveHandler.handle(operation, historicalCommit);

        verify(shopDao, times(1)).removeShop(any(Shop.class));
    }

    @Test
    void successCheckHandle() {
        Reference reference = new Reference();
        ShopConfigRef shopConfigRef = new ShopConfigRef();
        shopConfigRef.setId("test-shop-id");
        reference.setShopConfig(shopConfigRef);

        var operation = new FinalOperation();
        var removeOp = new RemoveOp();
        removeOp.setRef(reference);
        operation.setRemove(removeOp);

        assertTrue(shopRemoveHandler.isHandle(operation));
    }

    @Test
    void notSuccessCheckHandle() {
        var operation = new FinalOperation();
        var removeOp = new RemoveOp();
        Reference reference = new Reference();
        removeOp.setRef(reference);
        operation.setRemove(removeOp);

        assertFalse(shopRemoveHandler.isHandle(operation));
    }


}
