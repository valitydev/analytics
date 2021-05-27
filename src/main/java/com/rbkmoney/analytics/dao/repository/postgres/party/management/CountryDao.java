package com.rbkmoney.analytics.dao.repository.postgres.party.management;

import com.rbkmoney.analytics.domain.db.tables.pojos.Country;
import com.rbkmoney.analytics.domain.db.tables.records.CountryRecord;
import com.rbkmoney.dao.impl.AbstractGenericDao;
import org.jooq.Query;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;

import static com.rbkmoney.analytics.domain.db.Tables.COUNTRY;

@Component
public class CountryDao extends AbstractGenericDao {

    public CountryDao(DataSource postgresDatasource) {
        super(postgresDatasource);
    }

    public void saveCountry(Country country) {
        CountryRecord countryRecord = getDslContext().newRecord(COUNTRY, country);
        Query query = getDslContext()
                .insertInto(COUNTRY)
                .set(countryRecord);
        execute(query);
    }

    public void updateCountry(String countryId, Country country) {
        Query query = getDslContext().update(COUNTRY)
                .set(COUNTRY.VERSION_ID, country.getVersionId())
                .set(COUNTRY.NAME, country.getName())
                .set(COUNTRY.TRADE_BLOC, country.getTradeBloc())
                .where(COUNTRY.COUNTRY_ID.eq(countryId));
        execute(query);
    }

    public void removeCountry(Country country) {
        Query query = getDslContext().update(COUNTRY)
                .set(COUNTRY.DELETED, true)
                .set(COUNTRY.VERSION_ID, country.getVersionId())
                .where(COUNTRY.COUNTRY_ID.eq(country.getCountryId()));
        execute(query);
    }

}
