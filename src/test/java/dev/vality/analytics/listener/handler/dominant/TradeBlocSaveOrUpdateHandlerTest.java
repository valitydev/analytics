package dev.vality.analytics.listener.handler.dominant;

import dev.vality.analytics.dao.repository.postgres.party.management.TradeBlocDao;
import dev.vality.analytics.domain.db.tables.pojos.TradeBloc;
import dev.vality.analytics.utils.TestData;
import dev.vality.damsel.domain.DomainObject;
import dev.vality.damsel.domain.TradeBlocObject;
import dev.vality.damsel.domain_config_v2.Author;
import dev.vality.damsel.domain_config_v2.FinalInsertOp;
import dev.vality.damsel.domain_config_v2.FinalOperation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class TradeBlocSaveOrUpdateHandlerTest {

    @Mock
    private TradeBlocDao tradeBlocDao;

    private TradeBlocSaveOrUpdateHandler tradeBlocSaveOrUpdateHandler;

    private TradeBlocObject tradeBlocObject;
    private Author author;

    @BeforeEach
    void setUp() {
        tradeBlocObject = TestData.buildTradeBlocObject();
        author = TestData.buildAuthor();
        tradeBlocSaveOrUpdateHandler = new TradeBlocSaveOrUpdateHandler(tradeBlocDao);
    }

    @Test
    void shouldHandleInsertOp() {
        var operation = new FinalOperation();
        var insertOp = new FinalInsertOp();
        DomainObject domainObject = new DomainObject();
        domainObject.setTradeBloc(tradeBlocObject);
        insertOp.setObject(domainObject);
        operation.setInsert(insertOp);
        long versionId = 1L;

        tradeBlocSaveOrUpdateHandler.handle(operation, author, versionId);

        verify(tradeBlocDao, times(1)).saveTradeBloc(any(TradeBloc.class));
    }

    @Test
    void notSuccessCheckHandle() {
        var operation = new FinalOperation();
        var insertOp = new FinalInsertOp();
        DomainObject domainObject = new DomainObject();
        insertOp.setObject(domainObject);
        operation.setInsert(insertOp);

        assertFalse(tradeBlocSaveOrUpdateHandler.isHandle(operation));
    }

    @Test
    void successCheckHandle() {
        var operation = new FinalOperation();
        var insertOp = new FinalInsertOp();
        DomainObject domainObject = new DomainObject();
        domainObject.setTradeBloc(tradeBlocObject);
        insertOp.setObject(domainObject);
        operation.setInsert(insertOp);

        assertTrue(tradeBlocSaveOrUpdateHandler.isHandle(operation));
    }

    @Test
    void shouldConvertToDatabaseObject() {
        long versionId = 1L;

        TradeBloc tradeBloc = tradeBlocSaveOrUpdateHandler.convertToDatabaseObject(tradeBlocObject, author, versionId);

        assertNotNull(tradeBloc);
        assertEquals(tradeBlocObject.getData().getName(), tradeBloc.getName());
        assertEquals(tradeBlocObject.getData().getDescription(), tradeBloc.getDescription());
        assertEquals(versionId, tradeBloc.getVersionId());
        assertEquals(tradeBlocObject.getRef().getId(), tradeBloc.getTradeBlocId());

    }
}