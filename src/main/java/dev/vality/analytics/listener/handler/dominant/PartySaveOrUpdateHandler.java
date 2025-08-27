package dev.vality.analytics.listener.handler.dominant;

import dev.vality.analytics.dao.repository.postgres.party.management.PartyDao;
import dev.vality.analytics.domain.db.enums.Blocking;
import dev.vality.analytics.domain.db.enums.Suspension;
import dev.vality.analytics.domain.db.tables.pojos.Party;
import dev.vality.analytics.listener.handler.dominant.common.AbstractDominantHandler;
import dev.vality.analytics.listener.handler.merger.PartyEventMerger;
import dev.vality.damsel.domain.DomainObject;
import dev.vality.damsel.domain.PartyConfigObject;
import dev.vality.damsel.domain_config_v2.FinalOperation;
import dev.vality.damsel.domain_config_v2.HistoricalCommit;
import dev.vality.geck.common.util.TBaseUtil;
import dev.vality.geck.common.util.TypeUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

@Slf4j
@Component
@RequiredArgsConstructor
public class PartySaveOrUpdateHandler extends AbstractDominantHandler.SaveOrUpdateHandler {

    private final PartyDao partyDao;
    private final PartyEventMerger partyEventMerger;

    @Override
    @Transactional
    public void handle(FinalOperation operation, HistoricalCommit historicalCommit) {
        var partyConfigObject = extract(operation).getPartyConfig();
        var newPartyData = convertToDatabaseObject(partyConfigObject, historicalCommit);
        var mergedParty = partyEventMerger.mergeParty(partyConfigObject.getRef().getId(), newPartyData);

        if (operation.isSetInsert()) {
            log.info(
                    "Save partyConfigObject operation. id='{}' version='{}'",
                    partyConfigObject.getRef().getId(), historicalCommit.getVersion()
            );
            mergedParty.setCreatedAt(TypeUtil.stringToLocalDateTime(historicalCommit.getCreatedAt()));
            mergedParty.setDeleted(false);
            partyDao.saveParty(mergedParty);
        } else if (operation.isSetUpdate()) {
            log.info(
                    "Update partyConfigObject operation. id='{}' version='{}'",
                    partyConfigObject.getRef().getId(), historicalCommit.getVersion()
            );
            partyDao.saveParty(mergedParty);
        }
    }

    @Override
    public boolean isHandle(FinalOperation change) {
        return matches(change, DomainObject::isSetPartyConfig);
    }

    private Party convertToDatabaseObject(PartyConfigObject partyConfigObject, HistoricalCommit historicalCommit) {
        var partyConfig = partyConfigObject.getData();
        LocalDateTime eventTime = TypeUtil.stringToLocalDateTime(historicalCommit.getCreatedAt());

        Party party = new Party();
        party.setPartyId(partyConfigObject.getRef().getId());
        party.setVersionId(historicalCommit.getVersion());
        party.setEventId(historicalCommit.getVersion());
        party.setEventTime(eventTime);
        party.setEmail(partyConfig.getContactInfo().getRegistrationEmail());

        if (partyConfig.isSetBlock()) {
            var blocking = partyConfig.getBlock();
            party.setBlocking(TBaseUtil.unionFieldToEnum(blocking, Blocking.class));
            if (blocking.isSetBlocked()) {
                party.setBlockedReason(partyConfig.getBlock().getBlocked().getReason());
                party.setBlockedSince(TypeUtil.stringToLocalDateTime(partyConfig.getBlock().getBlocked().getSince()));
            }
            if (blocking.isSetUnblocked()) {
                party.setUnblockedReason(partyConfig.getBlock().getUnblocked().getReason());
                party.setUnblockedSince(TypeUtil.stringToLocalDateTime(partyConfig.getBlock().getUnblocked().getSince()));
            }
        }

        if (partyConfig.isSetSuspension()) {
            var partySuspension = partyConfig.getSuspension();
            party.setSuspension(TBaseUtil.unionFieldToEnum(partySuspension, Suspension.class));
            if (partySuspension.isSetActive()) {
                party.setSuspensionActiveSince(TypeUtil.stringToLocalDateTime(partySuspension.getActive().getSince()));
            }
            if (partySuspension.isSetSuspended()) {
                party.setSuspensionSuspendedSince(
                        TypeUtil.stringToLocalDateTime(partySuspension.getSuspended().getSince())
                );
            }
        }

        // Метаданные изменения
        var changedBy = historicalCommit.getChangedBy();
        party.setChangedById(changedBy.getId());
        party.setChangedByName(changedBy.getName());
        party.setChangedByEmail(changedBy.getEmail());

        return party;
    }
}
