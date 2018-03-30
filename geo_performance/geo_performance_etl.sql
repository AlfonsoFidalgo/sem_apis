INSERT INTO reports.sem_geo_performance
		SELECT
			geo.month,
      acc.country_code as code2,
      lac.iso_3 as code3,
			geo.account,
			geo.campaign,
			geo.adgroup,
			geo.campaign_id,
			geo.adgroup_id,
      geo.device,
      geo.impressions,
			geo.clicks,
      geo.wpos,
			geo.conversions,
			geo.cost * (CASE WHEN geo.currency = 'USD' THEN (
				SELECT rate FROM reports.lookup_latest_rates
				WHERE from_currency_id = 3
			)
				ELSE 1 END) as cost_gbp,
			geo.engine
		FROM reports.sem_geo_raw_upload geo
		LEFT JOIN reports.adwords_country_codes acc ON acc.country_id = geo.country_id
		LEFT JOIN reports.lookup_aml_countries lac ON lac.iso_2 = acc.country_code;

--STEP 2: delete rows from raw table
DELETE FROM reports.sem_geo_raw_upload;
