/*
Script Name: demo.sql
Author: Matt DeMarco (matt@memsql.com)
Created: 01.24.2019
Updated: 01.24.2019

Purpose: simple kafka transform using public kafka source

*/
drop database if exists test;
create database if not exists test;
use test;

CREATE TABLE events (
    user_id int,
    event_name varchar(128),
    advertiser varchar(128),
    campaign int(11),
    gender varchar(128),
    income varchar(128),
    page_url varchar(128),
    region varchar(128),
    country varchar(128),
    KEY adtmidx (user_id,event_name,advertiser,campaign) USING CLUSTERED COLUMNSTORE,
    SHARD KEY user_id (user_id)
);

CREATE REFERENCE TABLE campaigns (
    campaign_id smallint(6) NOT NULL DEFAULT '0',
    campaign_name varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
    PRIMARY KEY (campaign_id)
);

INSERT INTO `campaigns` VALUES (1,'demand great'),(2,'blackout'),(3,'flame broiled'),(4,'take it from a fish'),(5,'thank you'),(6,'designed by you'),(7,'virtual launch'),(8,'ultra light'),(9,'warmth'),(10,'run healthy'),(11,'virtual city'),(12,'online lifestyle'),(13,'dream burger'),(14,'super bowl tweet');


create pipeline testpipe as
load data kafka 'public-kafka.memcompute.com:9092/ad_events'
with transform ('file:///tmp/demo.py','','') 
ignore
into table events
FIELDS TERMINATED BY '\t' ENCLOSED BY '' ESCAPED BY '\\'
LINES TERMINATED BY '\n' STARTING BY ''
(user_id,event_name,advertiser,campaign,gender,income,page_url,region,country);
;


alter pipeline testpipe set offsets latest;
test pipeline testpipe limit 10;
start pipeline testpipe;

# Check for any pipeline errors
select * from information_schema.pipelines_errors;

/* Sample queries */
SELECT COUNT(*)
FROM events;


/* What campaigns are we running? */
SELECT *
FROM campaigns;


/* a traditional funnel */
SELECT
    campaign,
    campaign_name,
    impression_count,
    click_count,
    downstream_conversion_count,
    click_count / impression_count AS conv_1,
    downstream_conversion_count / click_count AS conv_2,
    downstream_conversion_count / impression_count AS all_conv
FROM (
    SELECT campaign,
    SUM(CASE WHEN (event_name="Impression") THEN 1 ELSE null END) AS impression_count,
    SUM(CASE WHEN (event_name="Click") THEN 1 ELSE null END) AS click_count,
    SUM(CASE WHEN (event_name="Downstream Conversion") THEN 1 ELSE null END) AS downstream_conversion_count
    FROM events
    WHERE advertiser = "Starbucks"
    group by campaign
) tab
LEFT JOIN campaigns ON campaigns.campaign_id = campaign
ORDER BY all_conv desc;
