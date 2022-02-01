package dev.vality.analytics.listener.handler.party;

import dev.vality.damsel.payment_processing.PartyChange;
import dev.vality.machinegun.eventsink.MachineEvent;


public interface PartyManagementEventHandler {

    void handle(MachineEvent machineEvent, PartyChange change);

}
