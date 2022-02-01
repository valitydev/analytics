package dev.vality.analytics.dao.repository.postgres.party.management;

import dev.vality.analytics.domain.db.tables.pojos.Contract;
import dev.vality.analytics.domain.db.tables.records.ContractRecord;
import dev.vality.dao.impl.AbstractGenericDao;
import dev.vality.mapper.RecordRowMapper;
import org.jooq.Query;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;

import static dev.vality.analytics.domain.db.tables.Contract.CONTRACT;

@Component
public class ContractDao extends AbstractGenericDao {

    private final RowMapper<Contract> contractRefRowMapper;

    public ContractDao(DataSource postgresDatasource) {
        super(postgresDatasource);
        this.contractRefRowMapper = new RecordRowMapper<>(CONTRACT, Contract.class);
    }

    public void saveContract(Contract contract) {
        final ContractRecord record = getDslContext().newRecord(CONTRACT, contract);
        Query queries = getDslContext()
                .insertInto(CONTRACT).set(record)
                .onConflict(CONTRACT.PARTY_ID, CONTRACT.CONTRACT_ID)
                .doUpdate()
                .set(record);
        execute(queries);
    }

    public Contract getContractByPartyIdAndContractId(String partyId, String contractId) {
        Query query = getDslContext().selectFrom(CONTRACT)
                .where(CONTRACT.CONTRACT_ID.eq(contractId)
                        .and(CONTRACT.PARTY_ID.eq(partyId)));
        return fetchOne(query, contractRefRowMapper);
    }

}
