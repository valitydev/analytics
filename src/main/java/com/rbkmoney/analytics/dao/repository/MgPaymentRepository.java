package com.rbkmoney.analytics.dao.repository;

import com.rbkmoney.analytics.dao.mapper.CommonRowsMapper;
import com.rbkmoney.analytics.dao.model.*;
import com.rbkmoney.analytics.dao.utils.DateFilterUtils;
import com.rbkmoney.analytics.dao.utils.QueryUtils;
import com.rbkmoney.damsel.analytics.SplitUnit;
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
public class MgPaymentRepository {

    public static final String SHOP_ID = "shopId";
    public static final String PARTY_ID = "partyId";

    private final JdbcTemplate jdbcTemplate;

    private final CommonRowsMapper<Cost> costCommonRowsMapper;
    private final CommonRowsMapper<CountModel> countModelCommonRowsMapper;
    private final CommonRowsMapper<NamingDistribution> namingDistributionCommonRowsMapper;
    private final CommonRowsMapper<SplitCost> splitCostCommonRowsMapper;

    public void insertBatch(List<MgPaymentSinkRow> mgPaymentSinkRows) {
        if (mgPaymentSinkRows != null && !mgPaymentSinkRows.isEmpty()) {
            jdbcTemplate.batchUpdate(MgPaymentBatchPreparedStatementSetter.INSERT, new MgPaymentBatchPreparedStatementSetter(mgPaymentSinkRows));
        }
    }

    public List<Cost> getAveragePayment(String partyId,
                                        List<String> shopIds,
                                        Long from,
                                        Long to) {
        Date dateFrom = DateFilterUtils.parseDate(from);
        Date dateTo = DateFilterUtils.parseDate(to);

        String selectSql = "SELECT currency, sum(amount * sign) / sum(sign) as amount " +
                "from analytic.events_sink ";
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

    public List<Cost> getPaymentsAmount(String partyId,
                                        List<String> shopIds,
                                        Long from,
                                        Long to) {
        Date dateFrom = DateFilterUtils.parseDate(from);
        Date dateTo = DateFilterUtils.parseDate(to);

        String selectSql = "SELECT currency, sum(amount * sign) as amount " +
                "from analytic.events_sink ";
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

    public List<CountModel> getPaymentsCount(String partyId,
                                             List<String> shopIds,
                                             Long from,
                                             Long to) {
        Date dateFrom = DateFilterUtils.parseDate(from);
        Date dateTo = DateFilterUtils.parseDate(to);

        String selectSql = "SELECT currency, sum(sign) as count " +
                "from analytic.events_sink ";
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
        return countModelCommonRowsMapper.map(rows);
    }

    public List<SplitCost> getPaymentsSplitAmount(String partyId,
                                                  List<String> shopIds,
                                                  Long from,
                                                  Long to,
                                                  SplitUnit splitUnit) {
        String groupBy = initGroupByFunction(splitUnit);

        String selectSql = "SELECT " + groupBy + " as unit , currency, sum(amount * sign) as amount " +
                "from analytic.events_sink ";
        String whereSql = "where timestamp >= ? and timestamp <= ? AND eventTimeHour >= ? AND eventTimeHour <= ? AND eventTime >= ? AND eventTime <= ?";
        String groupedSql = " group by partyId, currency, " + groupBy +
                " having partyId = ? " +
                " AND sum(sign) > 0";

        String sql = selectSql;
        List<Object> params = null;

        Date dateFrom = DateFilterUtils.parseDate(from);
        Date dateTo = DateFilterUtils.parseDate(to);
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
        return splitCostCommonRowsMapper.map(rows);
    }

    private String initGroupByFunction(SplitUnit splitUnit) {
        String groupBy;

        switch (splitUnit) {
            case MINUTE:
                groupBy = "toMinute(toDateTime(eventTime))";
                break;
            case DAY:
                groupBy = "toDay(toDate(eventTime))";
                break;
            case HOUR:
                groupBy = "toHour(toDateTime(eventTime))";
                break;
            case WEEK:
                groupBy = "toWeek(toDate(eventTime))";
                break;
            case YEAR:
                groupBy = "toYear(toDate(eventTime))";
                break;
            case MONTH:
                groupBy = "toMonth(toDate(eventTime))";
                break;
            default:
                throw new RuntimeException();
        }
        return groupBy;
    }

    public List<NamingDistribution> getPaymentsToolDistribution(String partyId,
                                                                List<String> shopIds,
                                                                Long from,
                                                                Long to) {
        return queryNamingDistributions(partyId, shopIds, from, to, "paymentTool");
    }

    public List<NamingDistribution> getPaymentsErrorDistribution(String partyId,
                                                                 List<String> shopIds,
                                                                 Long from,
                                                                 Long to) {
        return queryNamingDistributions(partyId, shopIds, from, to, "errorReason");
    }

    private List<NamingDistribution> queryNamingDistributions(String partyId, List<String> shopIds, Long from, Long to, String name) {
        Object[] params = null;
        Date dateFrom = DateFilterUtils.parseDate(from);
        Date dateTo = DateFilterUtils.parseDate(to);

        String sql = "SELECT %1$s, %3$s as naming_result," +
                "(SELECT sum(sign) from analytic.events_sink " +
                "where status='failed' AND timestamp >= ? and timestamp <= ? AND eventTimeHour >= ? AND eventTimeHour <= ? AND eventTime >= ? AND eventTime <= ? AND %1$s %2$s " +
                "group by partyId " +
                "having sum(sign) > 0) as total_count, sum(sign) * 100 / total_count as percent " +
                "from analytic.events_sink " +
                "where status='failed' AND timestamp >= ? and timestamp <= ? AND eventTimeHour >= ? AND eventTimeHour <= ? AND eventTime >= ? AND eventTime <= ? AND %1$s %2$s " +
                "group by %1$s, %3$s " +
                "having sum(sign) > 0";

        if (!CollectionUtils.isEmpty(shopIds)) {

            StringBuilder inList = QueryUtils.generateInList(shopIds);
            sql = String.format(sql, SHOP_ID, inList.toString(), name);

            List<Object> list = new ArrayList<>(Arrays.asList(dateFrom, dateTo, from, to, from, to));
            list.addAll(shopIds);
            list.addAll(list);

            params = list.toArray();
        } else {
            sql = String.format(sql, PARTY_ID, " = ? ", name);
            params = new Object[]{dateFrom, dateTo, from, to, from, to, partyId, dateFrom, dateTo, from, to, from, to, partyId};
        }

        List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql, params);

        return namingDistributionCommonRowsMapper.map(rows);
    }

}
