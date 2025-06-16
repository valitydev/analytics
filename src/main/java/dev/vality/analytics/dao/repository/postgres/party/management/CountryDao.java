package dev.vality.analytics.dao.repository.postgres.party.management;

import dev.vality.analytics.domain.db.tables.pojos.Country;
import dev.vality.analytics.domain.db.tables.records.CountryRecord;
import dev.vality.dao.impl.AbstractGenericDao;
import org.jooq.Query;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;

import static dev.vality.analytics.domain.db.Tables.COUNTRY;

@Component
public class CountryDao extends AbstractGenericDao {

    public CountryDao(DataSource dataSource) {
        super(dataSource);
    }

    public void saveCountry(Country country) {
        CountryRecord countryRecord = getDslContext().newRecord(COUNTRY, country);
        Query query = getDslContext()
                .insertInto(COUNTRY)
                .set(countryRecord)
                .onConflictDoNothing();
        execute(query);
    }

    public void updateCountry(Country country) {
        Query query = getDslContext().update(COUNTRY)
                .set(COUNTRY.VERSION_ID, country.getVersionId())
                .set(COUNTRY.NAME, country.getName())
                .set(COUNTRY.TRADE_BLOC, country.getTradeBloc())
                .set(COUNTRY.CHANGED_BY_ID, country.getChangedById())
                .set(COUNTRY.CHANGED_BY_NAME, country.getChangedByName())
                .set(COUNTRY.CHANGED_BY_EMAIL, country.getChangedByEmail())
                .where(COUNTRY.COUNTRY_ID.eq(country.getCountryId()));
        execute(query);
    }

    public void removeCountry(Country country) {
        Query query = getDslContext().update(COUNTRY)
                .set(COUNTRY.DELETED, true)
                .set(COUNTRY.VERSION_ID, country.getVersionId())
                .set(COUNTRY.CHANGED_BY_ID, country.getChangedById())
                .set(COUNTRY.CHANGED_BY_NAME, country.getChangedByName())
                .set(COUNTRY.CHANGED_BY_EMAIL, country.getChangedByEmail())
                .where(COUNTRY.COUNTRY_ID.eq(country.getCountryId()));
        execute(query);
    }

}
