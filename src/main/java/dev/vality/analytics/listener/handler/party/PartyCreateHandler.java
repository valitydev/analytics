package dev.vality.analytics.listener.handler.party;

import dev.vality.analytics.constant.EventType;
import dev.vality.analytics.dao.repository.postgres.party.management.PartyDao;
import dev.vality.analytics.domain.db.enums.Blocking;
import dev.vality.analytics.domain.db.enums.Suspension;
import dev.vality.analytics.domain.db.tables.pojos.Party;
import dev.vality.analytics.listener.handler.ChangeHandler;
import dev.vality.damsel.payment_processing.PartyChange;
import dev.vality.damsel.payment_processing.PartyCreated;
import dev.vality.geck.common.util.TypeUtil;
import dev.vality.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

@Slf4j
@Order(1)
@Component
@RequiredArgsConstructor
public class PartyCreateHandler implements ChangeHandler<PartyChange, MachineEvent> {

    private final PartyDao partyDao;

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void handleChange(PartyChange change, MachineEvent event) {
        PartyCreated partyCreated = change.getPartyCreated();
        LocalDateTime partyCreatedAt = TypeUtil.stringToLocalDateTime(partyCreated.getCreatedAt());
        Party party = new Party();
        party.setEventId(event.getEventId());
        party.setEventTime(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));
        party.setPartyId(partyCreated.getId());
        party.setCreatedAt(partyCreatedAt);
        party.setEmail(partyCreated.getContactInfo().getRegistrationEmail());
        party.setBlocking(Blocking.unblocked);
        party.setBlockedSince(partyCreatedAt);
        party.setSuspension(Suspension.active);
        party.setUnblockedSince(partyCreatedAt);
        party.setSuspensionActiveSince(partyCreatedAt);
        party.setRevisionId("0");
        party.setRevisionChangedAt(partyCreatedAt);

        partyDao.saveParty(party);

        log.debug("Party create event saveParty: {}", party);
    }

    @Override
    public EventType getChangeType() {
        return EventType.PARTY_CREATED;
    }
}
