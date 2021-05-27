package com.rbkmoney.analytics.listener.handler.dominant;

import com.rbkmoney.analytics.dao.repository.postgres.party.management.TradeBlocDao;
import com.rbkmoney.analytics.domain.db.tables.pojos.TradeBloc;
import com.rbkmoney.analytics.utils.TestData;
import com.rbkmoney.damsel.domain.DomainObject;
import com.rbkmoney.damsel.domain.TradeBlocObject;
import com.rbkmoney.damsel.domain_config.InsertOp;
import com.rbkmoney.damsel.domain_config.Operation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class TradeBlocDominantHandlerTest {

    @Mock
    private TradeBlocDao tradeBlocDao;

    private TradeBlocDominantHandler tradeBlocHandler;

    private TradeBlocObject tradeBlocObject;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        tradeBlocObject = TestData.buildTradeBlocObject();
        tradeBlocHandler = new TradeBlocDominantHandler(tradeBlocDao);
    }

    @Test
    void shouldHandleInsertOp() {
        Operation operation = new Operation();
        InsertOp insertOp = new InsertOp();
        DomainObject domainObject = new DomainObject();
        domainObject.setTradeBloc(tradeBlocObject);
        insertOp.setObject(domainObject);
        operation.setInsert(insertOp);
        long versionId = 1L;

        tradeBlocHandler.handle(operation, versionId);

        verify(tradeBlocDao, times(1)).saveTradeBloc(any(TradeBloc.class));
    }

    @Test
    void notSuccessCheckHandle() {
        Operation operation = new Operation();
        InsertOp insertOp = new InsertOp();
        DomainObject domainObject = new DomainObject();
        insertOp.setObject(domainObject);
        operation.setInsert(insertOp);

        assertFalse(tradeBlocHandler.isHandle(operation));
    }

    @Test
    void successCheckHandle() {
        Operation operation = new Operation();
        InsertOp insertOp = new InsertOp();
        DomainObject domainObject = new DomainObject();
        domainObject.setTradeBloc(tradeBlocObject);
        insertOp.setObject(domainObject);
        operation.setInsert(insertOp);

        assertTrue(tradeBlocHandler.isHandle(operation));
    }

    @Test
    void shouldConvertToDatabaseObject() {
        long versionId = 1L;

        TradeBloc tradeBloc = tradeBlocHandler.convertToDatabaseObject(versionId, tradeBlocObject);

        assertNotNull(tradeBloc);
        assertEquals(tradeBlocObject.getData().getName(), tradeBloc.getName());
        assertEquals(tradeBlocObject.getData().getDescription(), tradeBloc.getDescription());
        assertEquals(versionId, tradeBloc.getVersionId());
        assertEquals(tradeBlocObject.getRef().getId(), tradeBloc.getTradeBlocId());

    }
}