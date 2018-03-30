DELETE FROM reports.sem_slot_performance WHERE day >= current_date - 7;
INSERT INTO reports.sem_slot_performance
  SELECT
    ssr.day,
    ssr.account,
    ssr.campaign,
    ssr.adgroup,
    ssr.campaign_id,
    ssr.adgroup_id,
    ssr.device,
    ssr.engine,
    ssr.slot,
    ssr.impressions,
    ssr.clicks,
    ssr.cost * (CASE WHEN ssr.currency = 'USD' THEN (
          SELECT rate FROM reports.lookup_latest_rates
          WHERE from_currency_id = 3
        )
          ELSE 1 END) as cost,
    ssr.conversions,
    ssr.position
  FROM reports.sem_slot_raw_upload ssr;

DELETE FROM reports.sem_slot_raw_upload;
