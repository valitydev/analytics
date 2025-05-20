package dev.vality.analytics.dao.repository.postgres.party.management;

import dev.vality.analytics.domain.db.tables.pojos.TradeBloc;
import dev.vality.analytics.domain.db.tables.records.TradeBlocRecord;
import dev.vality.dao.impl.AbstractGenericDao;
import org.jooq.Query;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;

import static dev.vality.analytics.domain.db.Tables.TRADE_BLOC;

@Component
public class TradeBlocDao extends AbstractGenericDao {

    public TradeBlocDao(DataSource dataSource) {
        super(dataSource);
    }

    public void saveTradeBloc(TradeBloc tradeBloc) {
        TradeBlocRecord tradeBlocRecord = getDslContext().newRecord(TRADE_BLOC, tradeBloc);
        Query query = getDslContext()
                .insertInto(TRADE_BLOC)
                .set(tradeBlocRecord);
        execute(query);
    }

    public void updateTradeBloc(String tradeBlocId, TradeBloc tradeBloc) {
        Query query = getDslContext().update(TRADE_BLOC)
                .set(TRADE_BLOC.VERSION_ID, tradeBloc.getVersionId())
                .set(TRADE_BLOC.NAME, tradeBloc.getName())
                .set(TRADE_BLOC.DESCRIPTION, tradeBloc.getDescription())
                .where(TRADE_BLOC.TRADE_BLOC_ID.eq(tradeBlocId));
        execute(query);
    }

    public void removeTradeBloc(TradeBloc tradeBloc) {
        Query query = getDslContext().update(TRADE_BLOC)
                .set(TRADE_BLOC.DELETED, true)
                .set(TRADE_BLOC.VERSION_ID, tradeBloc.getVersionId())
                .where(TRADE_BLOC.TRADE_BLOC_ID.eq(tradeBloc.getTradeBlocId()));
        execute(query);
    }

}
