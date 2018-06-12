--QS metrics
DELETE FROM reports.sem_qs WHERE day >= current_date - 7;
INSERT INTO reports.sem_qs
		SELECT
      *
		FROM reports.sem_qs_upload;

DELETE FROM reports.sem_qs_upload;

--Top vs Other
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

--Rest
--STEP 1: Update SEM engine metrics
DELETE FROM reports.sem_adgroup_performance WHERE day >= current_date - 7;
INSERT INTO reports.sem_adgroup_performance
		SELECT
			aru.day,
			aru.account,
			aru.campaign,
			aru.adgroup,
			aru.campaign_id,
			aru.adgroup_id,
			aru.device,
			aru.impressions,
			aru.clicks,
			aru.avg_pos,
			aru.conversions,
			aru.cost_gbp * (CASE WHEN aru.currency = 'USD' THEN (
				SELECT rate FROM reports.lookup_latest_rates
				WHERE from_currency_id = 3
			)
				ELSE 1 END) as cost_gbp,
			aru.engine,
			aru.impression_share
		FROM reports.sem_adgroup_raw_upload aru;

--STEP 2: delete rows from raw table
DELETE FROM reports.sem_adgroup_raw_upload;

--STEP 3: Create temporary table
CREATE TEMPORARY TABLE tmp_sem_attribution (
	user_id BIGINT,
  source VARCHAR(15),
  content VARCHAR(100),
	engine VARCHAR(10),
	campaign_id BIGINT,
	adgroup_id BIGINT,
	profile_country VARCHAR(5),
	transfer_date TIMESTAMP,
	creation_platform VARCHAR(14),
	profile_type VARCHAR(10),
  first_paid_touch DOUBLE PRECISION,
  even_weights DOUBLE PRECISION,
  first_team_touch DOUBLE PRECISION,
	ltr_12m NUMERIC(20, 2),
	costs_12m NUMERIC(20, 2),
	ltv_12m NUMERIC(20, 2)
);


--STEP 4A: data from attribution metadata goes into tmp_sem_attribution and classified
INSERT INTO tmp_sem_attribution
		SELECT DISTINCT
			auj.user_id,
      auj.source,
      auj.content,
			(CASE
				WHEN auj.source ILIKE '%google%' THEN 'google'
				WHEN auj.source ILIKE '%bing%' THEN 'bing'
				WHEN auj.source ILIKE '%quora%' THEN 'quora'
				WHEN auj.source ILIKE '%uac%' THEN 'google'
				ELSE auj.source
			END) as engine,
			(CASE
				WHEN auj.campaign ILIKE '%quora%' THEN NULL
        WHEN auj.source ILIKE '%uac%' THEN auj.campaign ::BIGINT
			 	WHEN substring(auj.campaign from '\d*___\d*') <> '___'
 			  	THEN split_part(substring(auj.campaign from '\d*___\d*'), '___', 1)::BIGINT
				ELSE NULL
			 END) as campaign_id,
			(CASE
				WHEN auj.campaign ILIKE '%quora%' THEN NULL
			 	WHEN substring(auj.campaign from '\d*___\d*') <> '___'
 			  	THEN split_part(substring(auj.campaign from '\d*___\d*'), '___', 2)::BIGINT
				ELSE NULL
			END) as adgroup_id,
			aum.profile_country,
			aum.transfer_date,
			ruc.creation_platform,
			aum.profile_type,
      auj.first_paid_touch,
      auj.even_weights,
      auj.first_team_touch,
			ltv.ltr_12m,
			ltv.costs_12m,
			ltv.ltv_12m
		FROM reports.attribution_user_journey auj
		JOIN reports.attribution_user_meta aum ON aum.user_id = auj.user_id
    JOIN reports.report_user_characteristics ruc ON ruc.id_user = auj.user_id
		LEFT JOIN reports.report_ltv_12m ltv ON ltv.id_user = auj.user_id
		WHERE auj.medium = 'cpc'
		AND auj.first_paid_touch NOTNULL
		AND aum.transfer_date > current_date - 10
		AND aum.transfer_date < current_date;


--STEP 4B: delete data from last 15 days
DELETE FROM reports.sem_attribution_v2 sa WHERE date_trunc('day', sa.first_successful_payment_transferred) > current_date - 10;

--STEP 5: sem attribution data goes into its own table
INSERT INTO reports.sem_attribution_v2
    SELECT DISTINCT
      tsa.user_id,
      tsa.transfer_date,
      tsa.engine,
      tsa.campaign_id,
      tsa.adgroup_id,
      (CASE
        WHEN tsa.engine = 'quora' THEN NULL
        WHEN tsa.source ILIKE '%uac%' THEN (
          SELECT
            account
          FROM reports.lookup_uac_elements
          WHERE campaign_id = tsa.campaign_id
          LIMIT 1)
        WHEN tsa.engine = 'google' THEN (
          SELECT
            pse.account_name
          FROM reports.lookup_paid_search_elements pse
          WHERE pse.campaign_id = tsa.campaign_id
          AND pse.adgroup_id = tsa.adgroup_id
          LIMIT 1)
        WHEN tsa.engine = 'bing' THEN (
          SELECT
            lbe.account_name
          FROM reports.lookup_bing_elements lbe
          WHERE lbe.campaign_id = tsa.campaign_id
          AND lbe.adgroup_id = tsa.adgroup_id
          LIMIT 1
        )
        ELSE NULL
       END) as account_name,
      (
        CASE
          WHEN tsa.engine = 'quora' THEN NULL
          WHEN tsa.source ILIKE '%uac%' THEN (
            SELECT
              lue.campaign
            FROM reports.lookup_uac_elements lue
            WHERE lue.campaign_id = tsa.campaign_id
          )
          WHEN tsa.engine = 'google' THEN (
            SELECT
              pse.campaign_name
            FROM reports.lookup_paid_search_elements pse
            WHERE pse.campaign_id = tsa.campaign_id
            AND pse.adgroup_id = tsa.adgroup_id
            LIMIT 1
          )
          WHEN tsa.engine = 'bing' THEN (
            SELECT
              lbe.campaign_name
            FROM reports.lookup_bing_elements lbe
            WHERE lbe.campaign_id = tsa.campaign_id
            AND lbe.adgroup_id = tsa.adgroup_id
            LIMIT 1
          )
          ELSE NULL
        END
      ) as campaign_name,
      (CASE
        WHEN tsa.engine = 'quora' THEN NULL
        WHEN tsa.source ILIKE '%uac%' THEN NULL
        WHEN tsa.engine = 'google' THEN (
          SELECT
            pse.adgroup_name
          FROM reports.lookup_paid_search_elements pse
          WHERE pse.campaign_id = tsa.campaign_id
          AND pse.adgroup_id = tsa.adgroup_id
          LIMIT 1
        )
        WHEN tsa.engine = 'bing' THEN (
          SELECT
            lbe.adgroup_name
          FROM reports.lookup_bing_elements lbe
          WHERE lbe.campaign_id = tsa.campaign_id
          AND lbe.adgroup_id = tsa.adgroup_id
          LIMIT 1
        )
        ELSE NULL
       END) as adgroup_name,
      tsa.creation_platform,
      tsa.profile_country,
      tsa.ltr_12m,
      tsa.costs_12m,
      tsa.ltv_12m,
      tsa.first_paid_touch,
      tsa.even_weights,
      tsa.first_team_touch,
			(CASE
				WHEN tsa.profile_type = 'business' THEN 1
				ELSE 0
			 END) as business,
      tsa.content
    FROM tmp_sem_attribution tsa;

-- -- UPDATE LTVs
UPDATE reports.sem_attribution_v2 sa
  SET ltr_12m = (
      SELECT
        ltv.ltr_12m
      FROM reports.report_ltv_12m ltv
      WHERE ltv.id_user = sa.user_id
  )
WHERE sa.first_successful_payment_transferred > current_date - 60;

UPDATE reports.sem_attribution_v2 sa
  SET costs_12m = (
      SELECT
        ltv.costs_12m
      FROM reports.report_ltv_12m ltv
      WHERE ltv.id_user = sa.user_id
  )
WHERE sa.first_successful_payment_transferred > current_date - 60;

UPDATE reports.sem_attribution_v2 sa
  SET ltv_12m = (
      SELECT
        ltv.ltv_12m
      FROM reports.report_ltv_12m ltv
      WHERE ltv.id_user = sa.user_id
  )
WHERE sa.first_successful_payment_transferred > current_date - 60;


--SEM SUMMARY
DELETE FROM reports.sem_summary_v2 ss WHERE ss.day > current_date - 60;

INSERT INTO reports.sem_summary_v2
	WITH new_users AS (
		SELECT
		sa.first_successful_payment_transferred :: DATE AS day,
				(CASE
					WHEN sa.creation_platform ILIKE '%app%' THEN 'Mobile'
					WHEN sa.creation_platform ILIKE '%mobile%' THEN 'Mobile'
					ELSE 'Computers'
				 END) as device,
				COALESCE(sa.engine, 'UNKNOWN') as engine,
				COALESCE(sa.account_name,'UNKNOWN') as account_name,
				COALESCE(sa.campaign_name,'UNKNOWN') as campaign_name,
				COALESCE(sa.adgroup_name, 'UNKNOWN') as adgroup_name,
				SUM(sa.first_paid_touch)                               AS mnu,
				SUM(sa.business)	as business_users,
				SUM(sa.ltr_12m) as ltr_12m,
				SUM(sa.costs_12m) as costs_12m,
				SUM(sa.ltv_12m) as ltv_12m
			FROM reports.sem_attribution_v2 sa
			WHERE date_trunc('day', sa.first_successful_payment_transferred) > current_date - 60
			AND sa.first_paid_touch > 0
			GROUP BY 1, 2, 3, 4, 5, 6),

			stats AS (
				SELECT
					sap.day :: DATE,
					(CASE
						WHEN sap.device ILIKE '%mobile%' THEN 'Mobile'
						ELSE 'Computers'
					 END) as device,
					sap.engine,
					sap.account,
					sap.campaign,
					sap.adgroup,
					SUM(sap.impressions) AS impressions,
					SUM(sap.clicks)      AS clicks,
					SUM(sap.impressions * sap.avg_pos) AS wPos,
					SUM(sap.cost_gbp)    AS cost,
					SUM(sap.conversions) AS conversions,
					SUM(sap.impressions * sap.impression_share) AS wIS
				FROM reports.sem_adgroup_performance AS sap
				WHERE sap.day > current_date - 60
				GROUP BY 1, 2, 3, 4, 5, 6),

			qs AS (
				SELECT
					sem_qs.day,
					sem_qs.account,
					sem_qs.campaign,
					sem_qs.adgroup,
					(CASE
					 WHEN sem_qs.device ILIKE '%mobile%' THEN 'Mobile'
					 ELSE 'Computers'
					 END) as device,
					sum(sem_qs.impressions) as impressions_qs,
					sum(sem_qs.wqs)as wqs,
					sem_qs.engine
				FROM reports.sem_qs
				WHERE sem_qs.day > current_date - 60
		    GROUP BY 1,2,3,4,5,8
			),

			top_page AS (
				SELECT
					ssp.day,
					ssp.engine,
					ssp.account,
					ssp.campaign,
					ssp.adgroup,
					(CASE
						WHEN ssp.device ILIKE '%mobile%' THEN 'Mobile'
						ELSE 'Computers'
					 END) as device,
					 sum(ssp.impressions) as impressions_top,
					 sum(ssp.clicks) as clicks_top,
					 sum(ssp.cost) as cost_top,
					 sum(ssp.conversions) as conversions_top,
					 sum(ssp.position) as position_top
				FROM reports.sem_slot_performance ssp
				WHERE ssp.slot = 'Top'
        AND ssp.day > current_date - 60
				GROUP BY 1,2,3,4,5,6
			)

	SELECT
		COALESCE(stats.day, new_users.day) as day,
		COALESCE(stats.engine, new_users.engine) as engine,
		COALESCE(stats.account, new_users.account_name) as account,
		COALESCE(stats.campaign, new_users.campaign_name) as campaign,
		COALESCE(stats.adgroup, new_users.adgroup_name) as adgroup,
		COALESCE(stats.impressions, 0) as impressions,
		COALESCE(stats.clicks, 0) as clicks,
		COALESCE(stats.cost, 0) as cost,
		COALESCE(stats.conversions, 0) as conversions,
		COALESCE(new_users.mnu, 0) as new_users,
		COALESCE(stats.wPos, 0) as wPos,
		COALESCE(stats.device, new_users.device) as device,
		COALESCE(stats.wIS, 0) as wIS,
		COALESCE(new_users.business_users, 0) as business_users,
		COALESCE(qs.impressions_qs, 0) as impressions_qs,
		COALESCE(qs.wqs, 0) as wqs,
		COALESCE(top_page.impressions_top, 0) as top_page_impressions,
		COALESCE(top_page.clicks_top, 0) as top_page_clicks,
		COALESCE(top_page.cost_top, 0) as top_page_cost,
		COALESCE(top_page.conversions_top, 0) as top_page_conversions,
		COALESCE(top_page.position_top, 0) as top_page_wpos,
		COALESCE(new_users.ltr_12m, 0) as ltr_12m,
		COALESCE(new_users.costs_12m, 0) as costs_12m,
		COALESCE(new_users.ltv_12m, 0) as ltv_12m
	FROM stats
	FULL JOIN new_users ON (stats.day = new_users.day AND
														stats.device = new_users.device AND
														stats.engine = new_users.engine AND
														stats.account = new_users.account_name AND
														stats.campaign = new_users.campaign_name AND
														stats.adgroup = new_users.adgroup_name)
	LEFT JOIN qs ON (stats.day = qs.day AND
									 stats.device = qs.device AND
									 stats.engine = qs.engine AND
			             stats.account = qs.account AND
			             stats.campaign = qs.campaign AND
			             stats.adgroup = qs.adgroup)
	LEFT JOIN top_page ON ( stats.day = top_page.day AND
													stats.device = top_page.device AND
												  stats.engine = top_page.engine AND
												  stats.account = top_page.account AND
												  stats.campaign = top_page.campaign AND
												  stats.adgroup = top_page.adgroup);
