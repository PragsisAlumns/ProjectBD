

drop table piwik_total_count_page;
CREATE EXTERNAL TABLE piwik_total_count_page (
idpage BIGINT,
totalcount BIGINT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/piwik/totalcountpage';

drop table piwik_page_conversion;
CREATE EXTERNAL TABLE piwik_page_conversion (
idpage BIGINT,
numconversion BIGINT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/piwik/pageconversion';

INSERT OVERWRITE DIRECTORY '/piwik/conversionratiobypage'
SELECT piwik_total_count_page.idpage, (piwik_page_conversion.numconversion/piwik_total_count_page.totalcount) FROM piwik_total_count_page JOIN piwik_page_conversion ON (piwik_page_conversion.idpage = piwik_total_count_page.idpage);





