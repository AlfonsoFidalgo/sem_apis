--STEP 1: Updatte SEM engine metrics
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
	url_first_visit VARCHAR(2083),
	engine VARCHAR(10),
	creative_id BIGINT,
	campaign_id BIGINT,
	adgroup_id BIGINT,
	cntry_first_address VARCHAR(5)
);

--STEP 4A: data from attribution metadata goes into tmp_sem_attribution and classified
INSERT INTO tmp_sem_attribution
  SELECT
	 ram.id_user,
   ram.url_first_visit,
	 (CASE
      WHEN ram.attr_category_level_3 ILIKE '%google%' THEN 'google'
      WHEN ram.attr_category_level_3 ILIKE '%bing%' THEN 'bing'
			WHEN ram.attr_category_level_3 ILIKE '%Mobile App%' THEN 'google'
			WHEN ram.attr_category_level_3 ILIKE '%quora%' THEN 'quora'
			WHEN ram.url_first_visit ILIKE '%utm_source=google%' THEN 'google'
			WHEN ram.url_first_visit ILIKE '%utm_source=bing%' THEN 'bing'
	 		ELSE NULL
	 END) as engine,

	 (CASE
		  WHEN ram.attr_category_level_3 ILIKE '%quora%' THEN NULL
		  WHEN split_part(split_part(ram.url_first_visit, 'creative=',2),'&',1) NOT SIMILAR TO '\d*'
		    THEN NULL
	    WHEN length(split_part(split_part(ram.url_first_visit, 'creative=',2),'&',1))>0
				THEN split_part(split_part(ram.url_first_visit, 'creative=',2),'&',1)::BIGINT
		  ELSE NULL
		END) as creative_id,

	 (CASE
		  WHEN ram.attr_category_level_3 ILIKE '%quora%' THEN NULL
		  WHEN substring(ram.attr_category_level_3, '\d*___\d*') NOTNULL
			  THEN split_part(split_part(substring(ram.attr_category_level_3, '\d*___\d*'), '___', 1),'#',1)::BIGINT
	    WHEN length(split_part(split_part(ram.url_first_visit, 'campaignid=',2),'&',1)) > 0
		    THEN split_part(split_part(split_part(ram.url_first_visit, 'campaignid=',2),'&',1),'#',1)::BIGINT
			WHEN (ram.attr_category_level_3 ILIKE 'Mobile App%') THEN 0
		  ELSE NULL
		END) as campaign_id,

		(CASE
			WHEN ram.attr_category_level_3 ILIKE '%quora%' THEN NULL
 		 WHEN (substring(ram.attr_category_level_3, '\d*___\d*') NOTNULL AND ram.attr_category_level_3 NOT ILIKE '%quora%')
 		   THEN split_part(split_part(substring(ram.attr_category_level_3, '\d*___\d*'), '___', 2),'#',1)::BIGINT
 		 WHEN length(split_part(split_part(ram.url_first_visit, 'adgroupid=',2),'&',1))>0
 		   THEN split_part(split_part(split_part(ram.url_first_visit, 'adgroupid=',2),'&',1),'#',1)::BIGINT
 		 WHEN ram.attr_category_level_3 ILIKE 'Mobile App%' THEN 0
 		 ELSE NULL
 	  END) as adgroup_id

  FROM reports.report_attribution_metadata ram
  WHERE ram.attr_category_level_2 = 'Paid Search'
  AND date_trunc('day', ram.first_successful_payment_transferred) > current_date - 10
	AND date_trunc('day', ram.first_successful_payment_transferred) < current_date;


	--STEP 4B: delete data from last 15 days
	DELETE FROM reports.sem_attribution sa WHERE sa.first_successful_payment_transferred > current_date - 10;

	--STEP 5: sem attribution data goes into its own table
	INSERT INTO reports.sem_attribution
		SELECT
					ram.id_user,
					ram.date_user_created,
					ram.first_successful_payment_transferred,
					ram.url_first_visit,
					tsa.engine,
					tsa.creative_id,
					tsa.campaign_id,
					tsa.adgroup_id,
					--identify account with campaign-adgroup id and then creative_id
					(CASE
					 WHEN tsa.campaign_id = 0 AND ram.cntry_first_address = 'AUS' THEN 'AU | Consumer | Universal App Campaign | English'
					 WHEN tsa.campaign_id = 0 AND ram.cntry_first_address = 'GBR' THEN 'UK | Consumer | Universal App Campaigns | English'
					 WHEN tsa.campaign_id = 0 AND ram.cntry_first_address = 'BRA' THEN 'BR | Consumer | Universal App Campaign | Portuguese'
					 WHEN tsa.campaign_id = 0 AND ram.cntry_first_address IN ('USA', 'CAN') THEN 'US | Consumer | Universal App Campaigns | English'
					 WHEN tsa.campaign_id = 0 AND ram.cntry_first_address IN ('JPN', 'HKG', 'NZL', 'SGP') THEN 'All | Consumer | Universal App Campaign | English'
					 WHEN tsa.campaign_id = 0 AND ram.cntry_first_address NOT IN ('USA', 'CAN', 'AUS', 'GBR', 'JPN', 'HKG', 'NZL', 'SGP') THEN 'EU | Consumer | Universal App Campaign | English'
					 WHEN tsa.campaign_id NOTNULL AND tsa.engine = 'google' THEN (
						 SELECT pse.account_name FROM reports.lookup_paid_search_elements pse
						 WHERE tsa.campaign_id = pse.campaign_id
						 AND tsa.adgroup_id = pse.adgroup_id LIMIT 1
					 )
					 WHEN tsa.campaign_id NOTNULL AND tsa.engine = 'bing' THEN (
						 SELECT lbe.account_name FROM reports.lookup_bing_elements lbe
						 WHERE tsa.campaign_id = lbe.campaign_id
						 AND tsa.adgroup_id = lbe.adgroup_id LIMIT 1
					 )
					 WHEN tsa.creative_id NOTNULL AND tsa.engine = 'google' THEN (
						SELECT pse.account_name FROM reports.lookup_paid_search_elements pse
						WHERE tsa.creative_id = pse.ad_id LIMIT 1
					 )
					 WHEN tsa.creative_id NOTNULL AND tsa.engine = 'bing' THEN (
					  SELECT lbe.account_name FROM reports.lookup_bing_elements lbe
					  WHERE tsa.creative_id = lbe.ad_id LIMIT 1
				   )
					 ELSE NULL
					 END) as account_name,

					--identify campaign with campaign-adgroup id and then creative_id
					(CASE
						WHEN tsa.campaign_id = 0 THEN
 						 (CASE
 						 		WHEN ram.cntry_first_address = 'AUS' AND ram.creation_platform = 'iOS App' THEN 'AU | Consumer | iOS Universal App Campaign | English'
 								WHEN ram.cntry_first_address = 'AUS' AND ram.creation_platform = 'Android App' THEN 'AU | Consumer | Android Universal App Campaign | English'
 								WHEN ram.cntry_first_address = 'GBR' AND ram.creation_platform = 'iOS App' THEN 'UK | Consumer | iOS Universal App Campaign | English'
 								WHEN ram.cntry_first_address = 'GBR' AND ram.creation_platform = 'Android App' THEN 'UK | Consumer | Android Universal App Campaign | English'
								WHEN ram.cntry_first_address = 'BRA' AND ram.creation_platform = 'iOS App' THEN 'BR | Consumer | iOS Universal App Campaign | Portuguese'
								WHEN ram.cntry_first_address = 'BRA' AND ram.creation_platform = 'Android App' THEN 'BR | Consumer | Android Universal App Campaign | Portuguese'
 								WHEN ram.cntry_first_address IN ('USA', 'CAN') AND ram.creation_platform = 'iOS App' THEN 'US | Consumer | iOS Universal App Campaign | English'
 								WHEN ram.cntry_first_address IN ('USA', 'CAN') AND ram.creation_platform = 'Android App' THEN 'US | Consumer | Android Universal App Campaign | English'
 								WHEN ram.cntry_first_address IN ('JPN', 'HKG', 'NZL', 'SGP') AND ram.creation_platform = 'iOS App' THEN 'All | Consumer | iOS Universal App Campaign | English'
 								WHEN ram.cntry_first_address IN ('JPN', 'HKG', 'NZL', 'SGP') AND ram.creation_platform = 'Android App' THEN 'All | Consumer | Android Universal App Campaign | English'
								WHEN ram.cntry_first_address NOT IN ('USA', 'CAN', 'GBR', 'AUS', 'JPN', 'HKG', 'NZL', 'SGP') AND ram.creation_platform = 'iOS App' THEN 'EU | Consumer | iOS Universal App Campaign | English'
								WHEN ram.cntry_first_address NOT IN ('USA', 'CAN', 'GBR', 'AUS', 'JPN', 'HKG', 'NZL', 'SGP') AND ram.creation_platform = 'Android App' THEN 'EU | Consumer | Android Universal App Campaign | English'
 						 END)
					 WHEN tsa.campaign_id NOTNULL AND tsa.engine = 'google' THEN (
						 SELECT pse.campaign_name FROM reports.lookup_paid_search_elements pse
						 WHERE tsa.campaign_id = pse.campaign_id
						 AND tsa.adgroup_id = pse.adgroup_id LIMIT 1
					 )
					 WHEN tsa.campaign_id NOTNULL AND tsa.engine = 'bing' THEN (
						 SELECT lbe.campaign_name FROM reports.lookup_bing_elements lbe
						 WHERE tsa.campaign_id = lbe.campaign_id
						 AND tsa.adgroup_id = lbe.adgroup_id LIMIT 1
					 )
					 WHEN tsa.creative_id NOTNULL AND tsa.engine = 'google' THEN (
						SELECT pse.campaign_name FROM reports.lookup_paid_search_elements pse
						WHERE tsa.creative_id = pse.ad_id LIMIT 1
					 )
					 WHEN tsa.creative_id NOTNULL AND tsa.engine = 'bing' THEN (
						 SELECT lbe.campaign_name FROM reports.lookup_bing_elements lbe
						 WHERE tsa.creative_id = lbe.ad_id LIMIT 1
					 )
					 ELSE NULL
					 END) as campaign_name,

					--identify ad group with campaign-adgroup id and then creative_id
					(CASE
					 WHEN tsa.campaign_id = 0 THEN ''
					 WHEN tsa.campaign_id NOTNULL AND tsa.engine = 'google' THEN (
						 SELECT pse.adgroup_name FROM reports.lookup_paid_search_elements pse
						 WHERE tsa.campaign_id = pse.campaign_id
						 AND tsa.adgroup_id = pse.adgroup_id LIMIT 1
					 )
					 WHEN tsa.adgroup_id NOTNULL AND tsa.engine = 'bing' THEN (
						 SELECT lbe.adgroup_name FROM reports.lookup_bing_elements lbe
						 WHERE tsa.campaign_id = lbe.campaign_id
						 AND tsa.adgroup_id = lbe.adgroup_id LIMIT 1
					 )
					 WHEN tsa.creative_id NOTNULL AND tsa.engine = 'google' THEN (
						SELECT pse.adgroup_name FROM reports.lookup_paid_search_elements pse
						WHERE tsa.creative_id = pse.ad_id LIMIT 1
					)
					WHEN tsa.creative_id NOTNULL AND tsa.engine = 'bing' THEN (
						SELECT lbe.adgroup_name FROM reports.lookup_bing_elements lbe
						WHERE tsa.creative_id = lbe.ad_id LIMIT 1
					)
					ELSE NULL
					END) as adgroup_name,
					 ram.creation_platform as creation_platform,
					 ram.cntry_first_address
				FROM reports.report_attribution_metadata ram
				JOIN tmp_sem_attribution tsa ON tsa.user_id = ram.id_user
				WHERE ram.attr_category_level_2 = 'Paid Search'
				AND ram.first_successful_payment_transferred NOTNULL
				AND date_trunc('day', ram.first_successful_payment_transferred) > current_date - 10
				AND date_trunc('day', ram.first_successful_payment_transferred) < current_date;



--SEM SUMMARY
DELETE FROM reports.sem_summary ss WHERE ss.day > current_date - 10;

INSERT INTO reports.sem_summary
	WITH new_users AS (
	      SELECT
	        sa.first_successful_payment_transferred :: DATE AS day,
					(CASE
					 	WHEN sa.creation_platform ILIKE '%app%' THEN 'Mobile'
						WHEN sa.creation_platform ILIKE '%mobile%' THEN 'Mobile'
						ELSE 'Computers'
					 END) as device,
	        sa.engine,
	        sa.account_name,
	        sa.campaign_name,
	        sa.adgroup_name,
	        COUNT(sa.user_id)                               AS mnu
	      FROM reports.sem_attribution sa
				WHERE date_trunc('day', sa.first_successful_payment_transferred) > current_date - 10
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
					WHERE sap.day > current_date - 10
	        GROUP BY 1, 2, 3, 4, 5, 6)

	  SELECT
	    stats.day,
	    stats.engine,
	    stats.account,
	    stats.campaign,
	    stats.adgroup,
	    stats.impressions,
	    stats.clicks,
	    stats.cost,
	    stats.conversions,
	    new_users.mnu,
	    stats.wPos,
	    stats.device,
			stats.wIS
	  FROM stats
	    LEFT JOIN new_users ON (stats.day = new_users.day AND
															stats.device = new_users.device AND
	                            stats.engine = new_users.engine AND
	                            stats.account = new_users.account_name AND
	                            stats.campaign = new_users.campaign_name AND
	                            stats.adgroup = new_users.adgroup_name);
