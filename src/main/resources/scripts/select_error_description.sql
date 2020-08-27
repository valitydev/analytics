SELECT %3$s as naming_result,
(
    SELECT count() from analytic.events_sink
    where status='failed' and timestamp >= ? and timestamp <= ? AND eventTimeHour >= ? AND eventTimeHour <= ? AND eventTime >= ? AND eventTime <= ? AND %1$s %2$s
) as total_count, count() * 100 / total_count as percent
from analytic.events_sink
where status='failed' and timestamp >= ? and timestamp <= ? AND eventTimeHour >= ? AND eventTimeHour <= ? AND eventTime >= ? AND eventTime <= ? AND %1$s %2$s
group by %3$s