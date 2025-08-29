package dev.vality.analytics.listener.handler.dominant;

import dev.vality.analytics.dao.repository.postgres.party.management.CountryDao;
import dev.vality.analytics.domain.db.tables.pojos.Country;
import dev.vality.analytics.utils.TestData;
import dev.vality.damsel.domain.CountryObject;
import dev.vality.damsel.domain.DomainObject;
import dev.vality.damsel.domain.TradeBlocRef;
import dev.vality.damsel.domain_config_v2.FinalInsertOp;
import dev.vality.damsel.domain_config_v2.FinalOperation;
import dev.vality.damsel.domain_config_v2.HistoricalCommit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class CountrySaveOrUpdateHandlerTest {

    @Mock
    private CountryDao countryDao;

    private CountrySaveOrUpdateHandler saveOrUpdateHandler;

    private CountryObject countryObject;
    private HistoricalCommit historicalCommit;

    @BeforeEach
    void setUp() {
        countryObject = TestData.buildCountryObject();
        historicalCommit = TestData.buildHistoricalCommit();
        saveOrUpdateHandler = new CountrySaveOrUpdateHandler(countryDao);
    }

    @Test
    void shouldHandleInsertOp() {
        var operation = new FinalOperation();
        var insertOp = new FinalInsertOp();
        DomainObject domainObject = new DomainObject();
        domainObject.setCountry(countryObject);
        insertOp.setObject(domainObject);
        operation.setInsert(insertOp);

        saveOrUpdateHandler.handle(operation, historicalCommit);

        verify(countryDao, times(1)).saveCountry(any(Country.class));
    }

    @Test
    void successCheckHandle() {
        var operation = new FinalOperation();
        var insertOp = new FinalInsertOp();
        DomainObject domainObject = new DomainObject();
        domainObject.setCountry(countryObject);
        insertOp.setObject(domainObject);
        operation.setInsert(insertOp);

        assertTrue(saveOrUpdateHandler.isHandle(operation));
    }

    @Test
    void notSuccessCheckHandle() {
        var operation = new FinalOperation();
        var insertOp = new FinalInsertOp();
        DomainObject domainObject = new DomainObject();
        insertOp.setObject(domainObject);
        operation.setInsert(insertOp);

        assertFalse(saveOrUpdateHandler.isHandle(operation));
    }

    @Test
    void shouldConvertToDatabaseObject() {
        Country country = saveOrUpdateHandler.convertToDatabaseObject(countryObject, historicalCommit);

        assertNotNull(country);
        assertEquals(countryObject.getData().getName(), country.getName());
        assertEquals(historicalCommit.getVersion(), country.getVersionId());
        assertEquals(countryObject.getRef().getId().name(), country.getCountryId());
        assertArrayEquals(
                countryObject.getData().getTradeBlocs().stream().map(TradeBlocRef::getId).toArray(String[]::new),
                country.getTradeBloc());

    }
}