package dev.vality.analytics.listener.handler.dominant;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import dev.vality.analytics.dao.repository.postgres.party.management.PartyDao;
import dev.vality.analytics.domain.db.tables.pojos.Party;
import dev.vality.analytics.listener.handler.dominant.common.AbstractDominantHandler;
import dev.vality.damsel.domain.PartyConfigRef;
import dev.vality.damsel.domain.Reference;
import dev.vality.damsel.domain_config_v2.FinalOperation;
import dev.vality.damsel.domain_config_v2.HistoricalCommit;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class PartyRemoveHandler extends AbstractDominantHandler.RemoveHandler {

    private final PartyDao partyDao;

    @Override
    @Transactional
    public void handle(FinalOperation operation, HistoricalCommit historicalCommit) {
        var partyConfigRef = extract(operation).getPartyConfig();
        log.info("Remove party operation. id='{}' version='{}'", partyConfigRef.getId(), historicalCommit.getVersion());
        partyDao.removeParty(convertToDatabaseObject(partyConfigRef, historicalCommit));
    }

    @Override
    public boolean isHandle(FinalOperation change) {
        return matches(change, Reference::isSetPartyConfig);
    }

    private Party convertToDatabaseObject(PartyConfigRef partyConfigRef, HistoricalCommit historicalCommit) {
        var changedBy = historicalCommit.getChangedBy();
        var party = new Party();
        party.setVersionId(historicalCommit.getVersion());
        party.setPartyId(partyConfigRef.getId());
        party.setChangedById(changedBy.getId());
        party.setChangedByName(changedBy.getName());
        party.setChangedByEmail(changedBy.getEmail());
        return party;
    }
}
