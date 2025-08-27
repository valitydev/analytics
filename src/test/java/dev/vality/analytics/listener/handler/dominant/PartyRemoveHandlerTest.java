package dev.vality.analytics.listener.handler.dominant;

import dev.vality.analytics.dao.repository.postgres.party.management.PartyDao;
import dev.vality.analytics.domain.db.tables.pojos.Party;
import dev.vality.analytics.utils.TestData;
import dev.vality.damsel.domain.PartyConfigRef;
import dev.vality.damsel.domain.Reference;
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
class PartyRemoveHandlerTest {

    @Mock
    private PartyDao partyDao;

    private PartyRemoveHandler partyRemoveHandler;
    private HistoricalCommit historicalCommit;

    @BeforeEach
    void setUp() {
        historicalCommit = TestData.buildHistoricalCommit();
        partyRemoveHandler = new PartyRemoveHandler(partyDao);
    }

    @Test
    void shouldHandleRemoveOp() {

        Reference reference = new Reference();
        PartyConfigRef partyConfigRef = new PartyConfigRef();
        partyConfigRef.setId("test-party-id");
        reference.setPartyConfig(partyConfigRef);

        var operation = new FinalOperation();
        var removeOp = new RemoveOp();
        removeOp.setRef(reference);
        operation.setRemove(removeOp);

        partyRemoveHandler.handle(operation, historicalCommit);

        verify(partyDao, times(1)).removeParty(any(Party.class));
    }

    @Test
    void successCheckHandle() {
        Reference reference = new Reference();
        PartyConfigRef partyConfigRef = new PartyConfigRef();
        partyConfigRef.setId("test-party-id");
        reference.setPartyConfig(partyConfigRef);

        var operation = new FinalOperation();
        var removeOp = new RemoveOp();
        removeOp.setRef(reference);
        operation.setRemove(removeOp);

        assertTrue(partyRemoveHandler.isHandle(operation));
    }

    @Test
    void notSuccessCheckHandle() {
        var operation = new FinalOperation();
        var removeOp = new RemoveOp();
        Reference reference = new Reference();
        removeOp.setRef(reference);
        operation.setRemove(removeOp);

        assertFalse(partyRemoveHandler.isHandle(operation));
    }


}
