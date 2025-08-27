package dev.vality.analytics.listener.handler.dominant;

import java.time.LocalDateTime;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import dev.vality.analytics.dao.repository.postgres.party.management.PartyDao;
import dev.vality.analytics.domain.db.tables.pojos.Party;
import dev.vality.analytics.listener.handler.dominant.common.AbstractDominantHandler;
import dev.vality.analytics.listener.handler.merger.PartyEventMerger;
import dev.vality.damsel.domain.DomainObject;
import dev.vality.damsel.domain.PartyConfigObject;
import dev.vality.damsel.domain_config_v2.FinalOperation;
import dev.vality.damsel.domain_config_v2.HistoricalCommit;
import dev.vality.geck.common.util.TypeUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

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
        var newPartyData = convertToDatabaseObject(partyConfigObject, historicalCommit, operation);
        var mergedParty = partyEventMerger.mergeParty(partyConfigObject.getRef().getId(), newPartyData);
        
        if (operation.isSetInsert()) {
            log.info("Save partyConfigObject operation. id='{}' version='{}'", partyConfigObject.getRef().getId(), historicalCommit.getVersion());
            partyDao.saveParty(mergedParty);
        } else if (operation.isSetUpdate()) {
            log.info("Update partyConfigObject operation. id='{}' version='{}'", partyConfigObject.getRef().getId(), historicalCommit.getVersion());
            partyDao.saveParty(mergedParty);
        }
    }

    @Override
    public boolean isHandle(FinalOperation change) {
        return matches(change, DomainObject::isSetPartyConfig);
    }

    private Party convertToDatabaseObject(PartyConfigObject partyConfigObject, HistoricalCommit historicalCommit, FinalOperation operation) {
        var changedBy = historicalCommit.getChangedBy();
        var partyConfig = partyConfigObject.getData();
        // Используем время создания из HistoricalCommit
        LocalDateTime partyCreatedAt = TypeUtil.stringToLocalDateTime(historicalCommit.getCreatedAt());
        var party = new Party();
        // Основные поля события
        party.setVersionId(historicalCommit.getVersion());
        party.setEventId(historicalCommit.getVersion());
        party.setPartyId(partyConfigObject.getRef().getId());
        party.setEventTime(partyCreatedAt);
        
        // Поля из PartyConfig (конфигурационные данные)
        party.setEmail(partyConfig.getContactInfo().getRegistrationEmail());
        party.setCreatedAt(partyCreatedAt);
        party.setRevisionId(String.valueOf(historicalCommit.getVersion()));
        party.setRevisionChangedAt(partyCreatedAt);
        
        // Метаданные изменения
        party.setChangedById(changedBy.getId());
        party.setChangedByName(changedBy.getName());
        party.setChangedByEmail(changedBy.getEmail());
        party.setDeleted(false);
        
        // Состояние blocking/suspension управляется отдельными обработчиками
        // Только для новых party устанавливаем начальные значения
        // Для update эти поля НЕ устанавливаем - merger сохранит существующие значения
        
        return party;
    }
}
