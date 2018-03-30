DELETE FROM reports.sem_ltv;

INSERT INTO reports.sem_ltv
	WITH ltv AS (
		SELECT
			sa.engine,
			sa.account_name,
		  sa.campaign_name,
			(CASE
			 	WHEN sa.creation_platform ILIKE '%app%' THEN 'Mobile'
				WHEN sa.creation_platform ILIKE '%mobile%' THEN 'Mobile'
				ELSE 'Computers'
			 END) as device,
			COUNT(sa.user_id) as new_users,
			SUM(ltv12.ltr_12m) as LTR12,
			SUM(ltvB.ltr_12m) as LTR12resp,
			SUM(ltv12.costs_12m) as costs_12m,
			SUM(ltvB.costs_12m) as costs_12m_resp
		FROM reports.sem_attribution sa
	  LEFT JOIN reports.report_ltv_12m ltv12 ON ltv12.id_user = sa.user_id
	  LEFT JOIN reports.report_responsive_ltv ltvB ON ltvB.id_user = sa.user_id
		WHERE date_trunc('day', sa.first_successful_payment_transferred) > current_date - 45
		GROUP BY 1,2,3,4
	), stats AS (
		SELECT
		sap.engine,
		sap.account,
		sap.campaign,
		(CASE
		 	WHEN sap.device ILIKE '%mobile%' THEN 'Mobile'
			ELSE 'Computers'
		 END) as device,
		sum(sap.clicks) as clicks,
		sum(sap.cost_gbp) as cost_gbp,
		sum(sap.conversions) as conversions
		FROM reports.sem_adgroup_performance sap
		WHERE sap.day > current_date - 45
		GROUP BY 1,2,3,4
	)

SELECT
	stats.engine,
	stats.account,
	stats.campaign,
	(CASE
	 	WHEN stats.device ILIKE '%mobile%' THEN 'Mobile'
		ELSE 'Computers'
	 END) as device,
	sum(stats.clicks) as clicks,
	sum(cost_gbp) as cost_gbp,
	sum(conversions) as conversions ,
	sum(new_users) as new_users,
	sum(ltv.LTR12) AS LTR12,
	sum(ltv.LTR12resp) AS LTRres12,
	sum(ltv.costs_12m) as costs_12m,
	sum(ltv.costs_12m_resp) as costs_12m_resp
FROM stats
LEFT JOIN ltv ON ltv.engine = stats.engine AND
		ltv.account_name = stats.account AND
		ltv.campaign_name = stats.campaign AND
		ltv.device = stats.device
GROUP BY 1,2,3,4;
