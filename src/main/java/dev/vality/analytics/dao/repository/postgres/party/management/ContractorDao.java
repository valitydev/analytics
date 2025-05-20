package dev.vality.analytics.dao.repository.postgres.party.management;

import dev.vality.analytics.domain.db.tables.pojos.Contractor;
import dev.vality.analytics.domain.db.tables.records.ContractorRecord;
import dev.vality.dao.impl.AbstractGenericDao;
import dev.vality.mapper.RecordRowMapper;
import lombok.extern.slf4j.Slf4j;
import org.jooq.InsertOnDuplicateSetMoreStep;
import org.jooq.Query;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;

import static dev.vality.analytics.domain.db.Tables.CONTRACTOR;

@Slf4j
@Component
public class ContractorDao extends AbstractGenericDao {

    private final RowMapper<Contractor> currentContractorRowMapper;

    public ContractorDao(DataSource dataSource) {
        super(dataSource);
        this.currentContractorRowMapper = new RecordRowMapper<>(CONTRACTOR, Contractor.class);
    }

    public void saveContractor(Contractor currentContractor) {
        final ContractorRecord contractorRecord = getDslContext().newRecord(CONTRACTOR, currentContractor);
        final InsertOnDuplicateSetMoreStep<ContractorRecord> query = getDslContext().insertInto(CONTRACTOR)
                .set(contractorRecord)
                .onConflict(CONTRACTOR.PARTY_ID, CONTRACTOR.CONTRACTOR_ID)
                .doUpdate()
                .set(contractorRecord);
        execute(query);
    }

    public Contractor getContractorByPartyIdAndContractorId(String partyId, String contractorId) {
        Query query = getDslContext().selectFrom(CONTRACTOR)
                .where(CONTRACTOR.CONTRACTOR_ID.eq(contractorId)
                        .and(CONTRACTOR.PARTY_ID.eq(partyId)));
        final Contractor contractor = fetchOne(query, currentContractorRowMapper);
        log.debug("getContractorById: {} contractor: {}", contractorId, contractor);
        return contractor;
    }

}
