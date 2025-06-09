package dev.vality.analytics.listener.handler.dominant;

import dev.vality.analytics.dao.repository.postgres.party.management.CountryDao;
import dev.vality.analytics.domain.db.tables.pojos.Country;
import dev.vality.analytics.utils.TestData;
import dev.vality.damsel.domain.CountryObject;
import dev.vality.damsel.domain.DomainObject;
import dev.vality.damsel.domain.TradeBlocRef;
import dev.vality.damsel.domain_config_v2.Author;
import dev.vality.damsel.domain_config_v2.FinalInsertOp;
import dev.vality.damsel.domain_config_v2.FinalOperation;
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
    private Author author;

    @BeforeEach
    void setUp() {
        countryObject = TestData.buildCountryObject();
        author = TestData.buildAuthor();
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
        long versionId = 1L;

        saveOrUpdateHandler.handle(operation, author, versionId);

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
        long versionId = 1L;

        Country country = saveOrUpdateHandler.convertToDatabaseObject(countryObject, author, versionId);

        assertNotNull(country);
        assertEquals(countryObject.getData().getName(), country.getName());
        assertEquals(versionId, country.getVersionId());
        assertEquals(countryObject.getRef().getId().name(), country.getCountryId());
        assertArrayEquals(
                countryObject.getData().getTradeBlocs().stream().map(TradeBlocRef::getId).toArray(String[]::new),
                country.getTradeBloc());

    }
}