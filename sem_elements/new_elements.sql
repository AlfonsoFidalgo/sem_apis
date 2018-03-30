--Query to maintain Bing and AdWords lookup tables
--The query removes duplicate elements, so we can just
--upload everything from the API

CREATE TEMPORARY TABLE tmp_elements (
	account_name VARCHAR(500),
	campaign_name VARCHAR(500),
	adgroup_name VARCHAR(500),
	campaign_id BIGINT,
	adgroup_id BIGINT,
	ad_id BIGINT
);

--Bing
INSERT INTO tmp_elements
  SELECT DISTINCT * FROM reports.lookup_bing_elements;

DELETE FROM reports.lookup_bing_elements;

INSERT INTO reports.lookup_bing_elements
    SELECT * FROM tmp_elements;

--Empty table
DELETE FROM tmp_elements;

--AdWords
INSERT INTO tmp_elements
  SELECT DISTINCT * FROM reports.lookup_paid_search_elements;

DELETE FROM reports.lookup_paid_search_elements;

INSERT INTO reports.lookup_paid_search_elements
    SELECT * FROM tmp_elements;
