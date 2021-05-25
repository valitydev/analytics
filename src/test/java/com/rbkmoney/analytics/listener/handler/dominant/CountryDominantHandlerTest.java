package com.rbkmoney.analytics.listener.handler.dominant;

import com.rbkmoney.analytics.dao.repository.postgres.party.management.CountryDao;
import com.rbkmoney.analytics.domain.db.tables.pojos.Country;
import com.rbkmoney.analytics.utils.TestData;
import com.rbkmoney.damsel.domain.CountryObject;
import com.rbkmoney.damsel.domain.DomainObject;
import com.rbkmoney.damsel.domain.TradeBlocRef;
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

class CountryDominantHandlerTest {

    @Mock
    private CountryDao countryDao;

    private CountryDominantHandler countryHandler;

    private CountryObject countryObject;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        countryObject = TestData.buildCountryObject();
        countryHandler = new CountryDominantHandler(countryDao);
    }

    @Test
    void shouldHandleInsertOp() {
        Operation operation = new Operation();
        InsertOp insertOp = new InsertOp();
        DomainObject domainObject = new DomainObject();
        domainObject.setCountry(countryObject);
        insertOp.setObject(domainObject);
        operation.setInsert(insertOp);
        long versionId = 1L;

        countryHandler.handle(operation, versionId);

        verify(countryDao, times(1)).saveCountry(any(Country.class));
    }

    @Test
    void successCheckHandle() {
        Operation operation = new Operation();
        InsertOp insertOp = new InsertOp();
        DomainObject domainObject = new DomainObject();
        domainObject.setCountry(countryObject);
        insertOp.setObject(domainObject);
        operation.setInsert(insertOp);

        assertTrue(countryHandler.isHandle(operation));
    }

    @Test
    void notSuccessCheckHandle() {
        Operation operation = new Operation();
        InsertOp insertOp = new InsertOp();
        DomainObject domainObject = new DomainObject();
        insertOp.setObject(domainObject);
        operation.setInsert(insertOp);

        assertFalse(countryHandler.isHandle(operation));
    }

    @Test
    void shouldConvertToDatabaseObject() {
        long versionId = 1L;

        Country country = countryHandler.convertToDatabaseObject(versionId, countryObject);

        assertNotNull(country);
        assertEquals(countryObject.getData().getName(), country.getName());
        assertEquals(versionId, country.getVersionId());
        assertEquals(countryObject.getRef().getId().name(), country.getCountryId());
        assertArrayEquals(
                countryObject.getData().getTradeBlocs().stream().map(TradeBlocRef::getId).toArray(String[]::new),
                country.getTradeBloc());

    }
}