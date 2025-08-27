package dev.vality.analytics.listener.handler.dominant;

import dev.vality.analytics.dao.repository.postgres.party.management.TradeBlocDao;
import dev.vality.analytics.domain.db.tables.pojos.TradeBloc;
import dev.vality.analytics.listener.handler.dominant.common.AbstractDominantHandler;
import dev.vality.damsel.domain.Reference;
import dev.vality.damsel.domain.TradeBlocRef;
import dev.vality.damsel.domain_config_v2.FinalOperation;
import dev.vality.damsel.domain_config_v2.HistoricalCommit;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class TradeBlocRemoveHandler extends AbstractDominantHandler.RemoveHandler {

    private final TradeBlocDao tradeBlocDao;

    @Override
    public void handle(FinalOperation operation, HistoricalCommit historicalCommit) {
        var tradeBlocRef = extract(operation).getTradeBloc();
        log.info("Remove trade bloc operation. id='{}' version='{}'", tradeBlocRef.getId(), historicalCommit.getVersion());
        tradeBlocDao.removeTradeBloc(convertToDatabaseObject(tradeBlocRef, historicalCommit));
    }

    @Override
    public boolean isHandle(FinalOperation change) {
        return matches(change, Reference::isSetTradeBloc);
    }

    private TradeBloc convertToDatabaseObject(TradeBlocRef tradeBlocRef, HistoricalCommit historicalCommit) {
        var changedBy = historicalCommit.getChangedBy();
        TradeBloc tradeBloc = new TradeBloc();
        tradeBloc.setVersionId(historicalCommit.getVersion());
        tradeBloc.setTradeBlocId(tradeBlocRef.getId());
        tradeBloc.setChangedById(changedBy.getId());
        tradeBloc.setChangedByName(changedBy.getName());
        tradeBloc.setChangedByEmail(changedBy.getEmail());
        return tradeBloc;
    }
}
