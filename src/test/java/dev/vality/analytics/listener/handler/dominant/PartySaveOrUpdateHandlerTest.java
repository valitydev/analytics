package dev.vality.analytics.listener.handler.dominant;

import dev.vality.analytics.dao.repository.postgres.party.management.PartyDao;
import dev.vality.analytics.domain.db.tables.pojos.Party;
import dev.vality.analytics.listener.handler.merger.PartyEventMerger;
import dev.vality.analytics.utils.TestData;
import dev.vality.damsel.domain.DomainObject;
import dev.vality.damsel.domain.PartyConfigObject;
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
class PartySaveOrUpdateHandlerTest {

    @Mock
    private PartyDao partyDao;

    @Mock
    private PartyEventMerger partyEventMerger;

    private PartySaveOrUpdateHandler partySaveOrUpdateHandler;
    private PartyConfigObject partyConfigObject;
    private HistoricalCommit historicalCommit;

    @BeforeEach
    void setUp() {
        partyConfigObject = TestData.buildPartyConfigObject();
        historicalCommit = TestData.buildHistoricalCommit();
        partySaveOrUpdateHandler = new PartySaveOrUpdateHandler(partyDao, partyEventMerger);
    }

    @Test
    void shouldHandleInsertOp() {
        var operation = new FinalOperation();
        var insertOp = new FinalInsertOp();
        DomainObject domainObject = new DomainObject();
        domainObject.setPartyConfig(partyConfigObject);
        insertOp.setObject(domainObject);
        operation.setInsert(insertOp);

        when(partyEventMerger.mergeParty(anyString(), any(Party.class))).thenReturn(new Party());

        partySaveOrUpdateHandler.handle(operation, historicalCommit);

        verify(partyDao, times(1)).saveParty(any(Party.class));
        verify(partyEventMerger, times(1)).mergeParty(anyString(), any(Party.class));
    }

    @Test
    void shouldHandleUpdateOp() {
        var operation = new FinalOperation();
        var updateOp = new UpdateOp();
        DomainObject domainObject = new DomainObject();
        domainObject.setPartyConfig(partyConfigObject);
        updateOp.setObject(domainObject);
        operation.setUpdate(updateOp);

        when(partyEventMerger.mergeParty(anyString(), any(Party.class))).thenReturn(new Party());

        partySaveOrUpdateHandler.handle(operation, historicalCommit);

        verify(partyDao, times(1)).saveParty(any(Party.class));
        verify(partyEventMerger, times(1)).mergeParty(anyString(), any(Party.class));
    }

    @Test
    void successCheckHandle() {
        var operation = new FinalOperation();
        var insertOp = new FinalInsertOp();
        DomainObject domainObject = new DomainObject();
        domainObject.setPartyConfig(partyConfigObject);
        insertOp.setObject(domainObject);
        operation.setInsert(insertOp);

        assertTrue(partySaveOrUpdateHandler.isHandle(operation));
    }

    @Test
    void notSuccessCheckHandle() {
        var operation = new FinalOperation();
        var insertOp = new FinalInsertOp();
        DomainObject domainObject = new DomainObject();
        insertOp.setObject(domainObject);
        operation.setInsert(insertOp);

        assertFalse(partySaveOrUpdateHandler.isHandle(operation));
    }


}
