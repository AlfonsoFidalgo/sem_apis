--Looks into unattributed new users and gives it another try
--Sometimes the elements tables aren't updated, so this query will sort it out

UPDATE reports.sem_attribution sa
  SET account_name = (
    SELECT account_name FROM reports.lookup_paid_search_elements se
    WHERE se.campaign_id = sa.campaign_id AND se.adgroup_id = sa.adgroup_id LIMIT 1),
    campaign_name = (
    SELECT se.campaign_name FROM reports.lookup_paid_search_elements se
    WHERE se.campaign_id = sa.campaign_id AND se.adgroup_id = sa.adgroup_id LIMIT 1
    ),
    adgroup_name = (
    SELECT se.adgroup_name FROM reports.lookup_paid_search_elements se
    WHERE se.campaign_id = sa.campaign_id AND se.adgroup_id = sa.adgroup_id LIMIT 1
    )
WHERE sa.account_name ISNULL
AND sa.campaign_id NOTNULL
AND sa.engine = 'google'
AND sa.first_successful_payment_transferred >= '2018-03-01';


UPDATE reports.sem_attribution sa
  SET account_name = (
    SELECT account_name FROM reports.lookup_bing_elements se
    WHERE se.campaign_id = sa.campaign_id AND se.adgroup_id = sa.adgroup_id LIMIT 1),
    campaign_name = (
    SELECT se.campaign_name FROM reports.lookup_bing_elements se
    WHERE se.campaign_id = sa.campaign_id AND se.adgroup_id = sa.adgroup_id LIMIT 1
    ),
    adgroup_name = (
    SELECT se.adgroup_name FROM reports.lookup_bing_elements se
    WHERE se.campaign_id = sa.campaign_id AND se.adgroup_id = sa.adgroup_id LIMIT 1
    )
WHERE sa.account_name ISNULL
AND sa.campaign_id NOTNULL
AND sa.engine = 'bing'
AND sa.first_successful_payment_transferred >= '2018-03-01';
