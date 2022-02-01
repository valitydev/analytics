package dev.vality.analytics.listener.handler.party;

import dev.vality.analytics.constant.EventType;
import dev.vality.analytics.dao.repository.postgres.party.management.PartyDao;
import dev.vality.analytics.domain.db.tables.pojos.Party;
import dev.vality.analytics.listener.handler.ChangeHandler;
import dev.vality.analytics.listener.handler.merger.PartyEventMerger;
import dev.vality.damsel.domain.Suspension;
import dev.vality.damsel.payment_processing.PartyChange;
import dev.vality.geck.common.util.TBaseUtil;
import dev.vality.geck.common.util.TypeUtil;
import dev.vality.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Component
@RequiredArgsConstructor
public class PartySuspensionHandler implements ChangeHandler<PartyChange, MachineEvent> {

    private final PartyDao partyDao;
    private final PartyEventMerger partyEventMerger;

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void handleChange(PartyChange change, MachineEvent event) {
        Suspension partySuspension = change.getPartySuspension();
        String partyId = event.getSourceId();

        Party party = new Party();
        party.setPartyId(partyId);
        party.setEventId(event.getEventId());
        party.setEventTime(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));
        party.setSuspension(TBaseUtil.unionFieldToEnum(partySuspension,
                dev.vality.analytics.domain.db.enums.Suspension.class));
        if (partySuspension.isSetActive()) {
            party.setSuspensionActiveSince(TypeUtil.stringToLocalDateTime(partySuspension.getActive().getSince()));
        } else if (partySuspension.isSetSuspended()) {
            party.setSuspensionSuspendedSince(TypeUtil.stringToLocalDateTime(
                    partySuspension.getSuspended().getSince())
            );
        }

        final Party mergedParty = partyEventMerger.mergeParty(partyId, party);
        partyDao.saveParty(mergedParty);

        log.debug("Party suspension event saveParty: {}", party);
    }

    @Override
    public EventType getChangeType() {
        return EventType.PARTY_SUSPENSION;
    }
}
