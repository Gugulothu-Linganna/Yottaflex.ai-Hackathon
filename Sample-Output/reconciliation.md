# Reconciliation report

| check | result | detail |
|---|---|---|
| batch_row_accounting | PASS | 0 LOADED batches where rows_in != sum of outcomes |
| orders_equals_inserted_keys | PASS | orders=239 sum(rows_inserted)=239 |
| rejects_traceable_to_batches | PASS | rejects=28 sum(rows_rejected)=28 |
| referential_integrity_customer | PASS | 0 orders with unknown customer_id |
| current_row_is_latest_version | PASS | 0 orders not at their latest history version |
| amount_aggregate_matches_history | PASS | orders=17672.77 history(latest)=17672.77 |
| derived_amount_cents_consistent | PASS | 0 rows with inconsistent amount_cents |
| quarantined_batches_not_in_target | PASS | 0 orders sourced from quarantined batches |
| registry_matches_orders_columns | PASS | registry columns missing from orders: [] |
