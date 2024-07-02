SELECT
    shop_id,
    currency,
    sum_captured_payment - sum_succeeded_refund as num
FROM (
         SELECT
             shopId as shop_id,
             currency,
             sum(amount - systemFee - guaranteeDeposit) as sum_captured_payment
         FROM analytic.events_sink
         WHERE
           ? >= timestamp
           and status = 'captured'
           and partyId = ?
           %1$s
           %2$s
         GROUP BY shopId, currency
     ) as sum_captured_payment_query
     ANY LEFT JOIN (
        SELECT
            shopId as shop_id,
            currency,
            sum(amount + systemFee) as sum_succeeded_refund
        FROM analytic.events_sink_refund
        WHERE
            ? >= timestamp
            and status = 'succeeded'
            and partyId = ?
            %1$s
            %2$s
        GROUP BY shopId, currency
    ) as sum_succeeded_refund_query USING  shop_id, currency
