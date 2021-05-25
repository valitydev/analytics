package com.rbkmoney.analytics.listener.handler.dominant;

import com.rbkmoney.analytics.dao.repository.postgres.party.management.TradeBlocDao;
import com.rbkmoney.analytics.domain.db.tables.pojos.TradeBloc;
import com.rbkmoney.damsel.domain.DomainObject;
import com.rbkmoney.damsel.domain.TradeBlocObject;
import com.rbkmoney.damsel.domain_config.Operation;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Component
@RequiredArgsConstructor
public class TradeBlocDominantHandler extends AbstractDominantHandler {

    private final TradeBlocDao tradeBlocDao;

    @Override
    @Transactional
    public void handle(Operation operation, long versionId) {
        DomainObject dominantObject = getDominantObject(operation);
        TradeBlocObject tradeBloc = dominantObject.getTradeBloc();
        if (operation.isSetInsert()) {
            log.info("Save trade bloc operation. id='{}' version='{}'", tradeBloc.getRef().getId(), versionId);
            tradeBlocDao.saveTradeBloc(convertToDatabaseObject(versionId, tradeBloc));
        } else if (operation.isSetUpdate()) {
            log.info("Update trade bloc operation. id='{}' version='{}'", tradeBloc.getRef().getId(), versionId);
            DomainObject oldObject = operation.getUpdate().getOldObject();
            TradeBlocObject oldTradeBloc = oldObject.getTradeBloc();
            tradeBlocDao.updateTradeBloc(oldTradeBloc.getRef().getId(), convertToDatabaseObject(versionId, tradeBloc));
        } else if (operation.isSetRemove()) {
            log.info("Remove trade bloc operation. id='{}' version='{}'", tradeBloc.getRef().getId(), versionId);
            tradeBlocDao.removeTradeBloc(convertToDatabaseObject(versionId, tradeBloc));
        }
    }

    @Override
    public boolean isHandle(Operation operation) {
        DomainObject dominantObject = getDominantObject(operation);
        return dominantObject.isSetTradeBloc();
    }

    public TradeBloc convertToDatabaseObject(long versionId, TradeBlocObject tradeBlocObject) {
        TradeBloc tradeBloc = new TradeBloc();
        tradeBloc.setVersionId(versionId);
        tradeBloc.setTradeBlocId(tradeBlocObject.getRef().getId());
        com.rbkmoney.damsel.domain.TradeBloc data = tradeBlocObject.getData();
        tradeBloc.setName(data.getName());
        tradeBloc.setDescription(data.getDescription());
        return tradeBloc;
    }

}
