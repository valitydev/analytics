package dev.vality.analytics.listener.handler.dominant;

import dev.vality.analytics.dao.repository.postgres.party.management.TradeBlocDao;
import dev.vality.analytics.domain.db.tables.pojos.TradeBloc;
import dev.vality.analytics.listener.handler.dominant.common.AbstractDominantHandler;
import dev.vality.damsel.domain.DomainObject;
import dev.vality.damsel.domain.TradeBlocObject;
import dev.vality.damsel.domain_config_v2.FinalOperation;
import dev.vality.damsel.domain_config_v2.HistoricalCommit;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class TradeBlocSaveOrUpdateHandler extends AbstractDominantHandler.SaveOrUpdateHandler {

    private final TradeBlocDao tradeBlocDao;

    @Override
    public void handle(FinalOperation operation, HistoricalCommit historicalCommit) {
        var tradeBlocObject = extract(operation).getTradeBloc();
        if (operation.isSetInsert()) {
            log.info(
                    "Save trade bloc operation. id='{}' version='{}'",
                    tradeBlocObject.getRef().getId(), historicalCommit.getVersion()
            );
            tradeBlocDao.saveTradeBloc(convertToDatabaseObject(tradeBlocObject, historicalCommit));
        } else if (operation.isSetUpdate()) {
            log.info(
                    "Update trade bloc operation. id='{}' version='{}'",
                    tradeBlocObject.getRef().getId(), historicalCommit.getVersion()
            );
            tradeBlocDao.updateTradeBloc(convertToDatabaseObject(tradeBlocObject, historicalCommit));
        }
    }

    @Override
    public boolean isHandle(FinalOperation change) {
        return matches(change, DomainObject::isSetTradeBloc);
    }

    protected TradeBloc convertToDatabaseObject(TradeBlocObject tradeBlocObject, HistoricalCommit historicalCommit) {
        TradeBloc tradeBloc = new TradeBloc();
        tradeBloc.setVersionId(historicalCommit.getVersion());
        tradeBloc.setTradeBlocId(tradeBlocObject.getRef().getId());
        dev.vality.damsel.domain.TradeBloc data = tradeBlocObject.getData();
        tradeBloc.setName(data.getName());
        tradeBloc.setDescription(data.getDescription());
        var changedBy = historicalCommit.getChangedBy();
        tradeBloc.setChangedById(changedBy.getId());
        tradeBloc.setChangedByName(changedBy.getName());
        tradeBloc.setChangedByEmail(changedBy.getEmail());
        return tradeBloc;
    }
}
