package dev.vality.analytics.listener.handler.dominant;

import dev.vality.analytics.dao.repository.postgres.party.management.CountryDao;
import dev.vality.analytics.domain.db.tables.pojos.Country;
import dev.vality.analytics.listener.handler.dominant.common.AbstractDominantHandler;
import dev.vality.damsel.domain.CountryRef;
import dev.vality.damsel.domain.Reference;
import dev.vality.damsel.domain_config_v2.FinalOperation;
import dev.vality.damsel.domain_config_v2.HistoricalCommit;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class CountryRemoveHandler extends AbstractDominantHandler.RemoveHandler {

    private final CountryDao countryDao;

    @Override
    public void handle(FinalOperation operation, HistoricalCommit historicalCommit) {
        var countryRef = extract(operation).getCountry();
        log.info("Remove country operation. id='{}' version='{}'", countryRef.getId().name(), historicalCommit.getVersion());
        countryDao.removeCountry(convertToDatabaseObject(countryRef, historicalCommit));
    }

    @Override
    public boolean isHandle(FinalOperation change) {
        return matches(change, Reference::isSetCountry);
    }

    private Country convertToDatabaseObject(CountryRef countryRef, HistoricalCommit historicalCommit) {
        var changedBy = historicalCommit.getChangedBy();
        Country country = new Country();
        country.setVersionId(historicalCommit.getVersion());
        country.setCountryId(countryRef.getId().name());
        country.setChangedById(changedBy.getId());
        country.setChangedByName(changedBy.getName());
        country.setChangedByEmail(changedBy.getEmail());
        return country;
    }
}
