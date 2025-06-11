package dev.vality.analytics.listener.handler.dominant;

import dev.vality.analytics.dao.repository.postgres.party.management.CountryDao;
import dev.vality.analytics.domain.db.tables.pojos.Country;
import dev.vality.analytics.listener.handler.dominant.common.AbstractDominantHandler;
import dev.vality.damsel.domain.CountryRef;
import dev.vality.damsel.domain.Reference;
import dev.vality.damsel.domain_config_v2.Author;
import dev.vality.damsel.domain_config_v2.FinalOperation;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class CountryRemoveHandler extends AbstractDominantHandler.RemoveHandler {

    private final CountryDao countryDao;

    @Override
    public void handle(FinalOperation operation, Author changedBy, long versionId) {
        var countryRef = extract(operation).getCountry();
        log.info("Remove country operation. id='{}' version='{}'", countryRef.getId().name(), versionId);
        countryDao.removeCountry(convertToDatabaseObject(countryRef, changedBy, versionId));
    }

    @Override
    public boolean isHandle(FinalOperation change) {
        return matches(change, Reference::isSetCountry);
    }

    private Country convertToDatabaseObject(CountryRef countryRef, Author changedBy, long versionId) {
        Country country = new Country();
        country.setVersionId(versionId);
        country.setCountryId(countryRef.getId().name());
        country.setChangedById(changedBy.getId());
        country.setChangedByName(changedBy.getName());
        country.setChangedByEmail(changedBy.getEmail());
        return country;
    }
}
