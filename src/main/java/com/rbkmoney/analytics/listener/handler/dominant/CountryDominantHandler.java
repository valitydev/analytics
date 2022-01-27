package com.rbkmoney.analytics.listener.handler.dominant;

import com.rbkmoney.analytics.dao.repository.postgres.party.management.CountryDao;
import com.rbkmoney.analytics.domain.db.tables.pojos.Country;
import com.rbkmoney.damsel.domain.CountryObject;
import com.rbkmoney.damsel.domain.DomainObject;
import com.rbkmoney.damsel.domain.TradeBlocRef;
import com.rbkmoney.damsel.domain_config.Operation;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Component
@RequiredArgsConstructor
public class CountryDominantHandler extends AbstractDominantHandler {

    private final CountryDao countryDao;

    @Override
    @Transactional
    public void handle(Operation operation, long versionId) {
        DomainObject dominantObject = getDominantObject(operation);
        CountryObject country = dominantObject.getCountry();
        if (operation.isSetInsert()) {
            log.info("Save country operation. id='{}' version='{}'", country.getRef().getId().name(), versionId);
            countryDao.saveCountry(convertToDatabaseObject(versionId, country));
        } else if (operation.isSetUpdate()) {
            log.info("Update country operation. id='{}' version='{}'", country.getRef().getId().name(), versionId);
            DomainObject oldObject = operation.getUpdate().getOldObject();
            CountryObject oldCountry = oldObject.getCountry();
            countryDao.updateCountry(oldCountry.getRef().getId().name(), convertToDatabaseObject(versionId, country));
        } else if (operation.isSetRemove()) {
            log.info("Remove country operation. id='{}' version='{}'", country.getRef().getId().name(), versionId);
            countryDao.removeCountry(convertToDatabaseObject(versionId, country));
        }
    }

    @Override
    public boolean isHandle(Operation operation) {
        DomainObject dominantObject = getDominantObject(operation);
        return dominantObject.isSetCountry();
    }

    public Country convertToDatabaseObject(long versionId, CountryObject countryObject) {
        Country country = new Country();
        country.setVersionId(versionId);
        country.setCountryId(countryObject.getRef().getId().name());
        com.rbkmoney.damsel.domain.Country data = countryObject.getData();
        country.setName(data.getName());
        String[] tradeBlocs = data.isSetTradeBlocs() ? data.getTradeBlocs()
                .stream()
                .map(TradeBlocRef::getId)
                .toArray(String[]::new) : new String[0];
        country.setTradeBloc(tradeBlocs);
        return country;
    }

}
