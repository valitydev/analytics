package dev.vality.analytics.listener.handler.dominant;

import dev.vality.analytics.dao.repository.postgres.party.management.CountryDao;
import dev.vality.analytics.domain.db.tables.pojos.Country;
import dev.vality.analytics.listener.handler.dominant.common.AbstractDominantHandler;
import dev.vality.damsel.domain.CountryObject;
import dev.vality.damsel.domain.DomainObject;
import dev.vality.damsel.domain.TradeBlocRef;
import dev.vality.damsel.domain_config_v2.FinalOperation;
import dev.vality.damsel.domain_config_v2.HistoricalCommit;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class CountrySaveOrUpdateHandler extends AbstractDominantHandler.SaveOrUpdateHandler {

    private final CountryDao countryDao;

    @Override
    public void handle(FinalOperation operation, HistoricalCommit historicalCommit) {
        var countryObject = extract(operation).getCountry();
        if (operation.isSetInsert()) {
            log.info(
                    "Save country operation. id='{}' version='{}'",
                    countryObject.getRef().getId().name(), historicalCommit.getVersion()
            );
            countryDao.saveCountry(convertToDatabaseObject(countryObject, historicalCommit));
        } else if (operation.isSetUpdate()) {
            log.info(
                    "Update country operation. id='{}' version='{}'",
                    countryObject.getRef().getId().name(), historicalCommit.getVersion()
            );
            countryDao.updateCountry(convertToDatabaseObject(countryObject, historicalCommit));
        }
    }

    @Override
    public boolean isHandle(FinalOperation change) {
        return matches(change, DomainObject::isSetCountry);
    }

    protected Country convertToDatabaseObject(CountryObject countryObject, HistoricalCommit historicalCommit) {
        Country country = new Country();
        country.setVersionId(historicalCommit.getVersion());
        country.setCountryId(countryObject.getRef().getId().name());
        dev.vality.damsel.domain.Country data = countryObject.getData();
        country.setName(data.getName());
        String[] tradeBlocs = data.isSetTradeBlocs() ? data.getTradeBlocs()
                .stream()
                .map(TradeBlocRef::getId)
                .toArray(String[]::new) : new String[0];
        country.setTradeBloc(tradeBlocs);
        var changedBy = historicalCommit.getChangedBy();
        country.setChangedById(changedBy.getId());
        country.setChangedByName(changedBy.getName());
        country.setChangedByEmail(changedBy.getEmail());
        return country;
    }
}
