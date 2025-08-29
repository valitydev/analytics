package dev.vality.analytics.dao.repository.postgres.party.management;

import java.util.List;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.jooq.Query;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import static dev.vality.analytics.domain.db.Tables.PARTY;
import dev.vality.analytics.domain.db.tables.pojos.Party;
import dev.vality.analytics.domain.db.tables.records.PartyRecord;
import dev.vality.dao.impl.AbstractGenericDao;
import dev.vality.mapper.RecordRowMapper;

@Component
public class PartyDao extends AbstractGenericDao {

    private final RowMapper<Party> partyRowMapper;

    public PartyDao(DataSource dataSource) {
        super(dataSource);
        this.partyRowMapper = new RecordRowMapper<>(PARTY, Party.class);
    }

    public void saveParty(Party party) {
        PartyRecord partyRecord = getDslContext().newRecord(PARTY, party);
        Query query = getDslContext()
                .insertInto(PARTY).set(partyRecord)
                .onConflict(PARTY.PARTY_ID)
                .doUpdate()
                .set(partyRecord);
        execute(query);
    }

    public void saveParty(List<Party> partyList) {
        List<Query> queries = partyList.stream()
                .map(party -> getDslContext().newRecord(PARTY, party))
                .map(partyRecord -> getDslContext()
                        .insertInto(PARTY).set(partyRecord)
                        .onConflict(PARTY.PARTY_ID)
                        .doUpdate()
                        .set(partyRecord))
                .collect(Collectors.toList());
        batchExecute(queries);
    }

    public void removeParty(Party party) {
        Query query = getDslContext().update(PARTY)
                .set(PARTY.DELETED, true)
                .set(PARTY.VERSION_ID, party.getVersionId())
                .set(PARTY.CHANGED_BY_ID, party.getChangedById())
                .set(PARTY.CHANGED_BY_NAME, party.getChangedByName())
                .set(PARTY.CHANGED_BY_EMAIL, party.getChangedByEmail())
                .where(PARTY.PARTY_ID.eq(party.getPartyId()));
        execute(query);
    }

    public Party getPartyById(String partyId) {
        Query query = getDslContext().selectFrom(PARTY)
                .where(PARTY.PARTY_ID.eq(partyId));
        return fetchOne(query, partyRowMapper);
    }

}
