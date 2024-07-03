                SELECT
                  currency,
                  sum_payment_without_refund as num
                FROM
                  (
                    SELECT
                      currency,
                      sum_captured_payment - sum_succeeded_refund as sum_payment_without_refund
                    FROM
                      (
                        SELECT
                          currency,
                          sum(amount - systemFee - guaranteeDeposit) as sum_captured_payment
                        FROM
                          analytic.events_sink
                        WHERE
                          ? >= timestamp
                          and status = 'captured'
                          and partyId = ?
                          %1$s
                          %2$s
                        GROUP BY
                          currency
                      ) as sum_payment_query
                      ANY LEFT JOIN (
                        SELECT
                          currency,
                          sum(amount + systemFee) as sum_succeeded_refund
                        FROM
                          analytic.events_sink_refund
                        WHERE
                          ? >= timestamp
                          and status = 'succeeded'
                          and partyId = ?
                          %1$s
                          %2$s
                        GROUP BY
                          currency
                      ) as sum_refund_query USING currency
                  ) as sum_payment_without_refund_query