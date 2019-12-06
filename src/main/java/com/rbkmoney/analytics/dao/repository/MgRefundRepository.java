package com.rbkmoney.analytics.dao.repository;

import com.rbkmoney.analytics.dao.mapper.CommonRowsMapper;
import com.rbkmoney.analytics.dao.model.Cost;
import com.rbkmoney.analytics.dao.model.MgRefundRow;
import com.rbkmoney.analytics.dao.utils.DateFilterUtils;
import com.rbkmoney.analytics.dao.utils.QueryUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.sql.Date;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class MgRefundRepository {

    private final JdbcTemplate jdbcTemplate;
    private final CommonRowsMapper<Cost> costCommonRowsMapper;

    public void insertBatch(List<MgRefundRow> mgRefundRows) {
        if (mgRefundRows != null && !mgRefundRows.isEmpty()) {
            jdbcTemplate.batchUpdate(MgRefundBatchPreparedStatementSetter.INSERT, new MgRefundBatchPreparedStatementSetter(mgRefundRows));
        }
    }

    public List<Cost> getPaymentsAmount(String partyId,
                                        List<String> shopIds,
                                        Long from,
                                        Long to) {
        Date dateFrom = DateFilterUtils.parseDate(from);
        Date dateTo = DateFilterUtils.parseDate(to);

        String selectSql = "SELECT currency, sum(amount * sign) as amount " +
                "from analytic.events_sink_refund ";
        String whereSql = "where timestamp >= ? and timestamp <= ? AND eventTimeHour >= ? AND eventTimeHour <= ? AND eventTime >= ? AND eventTime <= ?";
        String groupedSql = " group by partyId, currency " +
                " having partyId = ? " +
                " AND sum(sign) > 0";

        String sql = selectSql;
        List<Object> params = null;

        if (!CollectionUtils.isEmpty(shopIds)) {
            StringBuilder inList = QueryUtils.generateInList(shopIds);
            sql = sql + whereSql + " AND shopId " + inList + groupedSql;
            params = new ArrayList<>(Arrays.asList(dateFrom, dateTo, from, to, from, to));
            params.addAll(shopIds);
            params.add(partyId);
        } else {
            sql = sql + whereSql + groupedSql;
            params = new ArrayList<>(Arrays.asList(dateFrom, dateTo, from, to, from, to, partyId));
        }

        List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql, params.toArray());
        return costCommonRowsMapper.map(rows);
    }

}
