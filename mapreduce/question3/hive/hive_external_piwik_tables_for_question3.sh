#########################
#     paths_by_visit    #
#########################
drop table piwik_paths_by_visit;
CREATE EXTERNAL TABLE piwik_paths_by_visit (
idvisit BIGINT,
paths STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/piwik/paths_by_visit';
show piwik_paths_by_visit;

#########################
#     log_conversion    #
#########################
drop table piwik_log_conversion;
CREATE EXTERNAL TABLE piwik_log_conversion (
idvisit BIGINT,
idvisitor STRING,
idaction_url STRING,
url STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/piwik/log_conversion';
show tables piwik_log_conversion;

INSERT OVERWRITE DIRECTORY '/piwik/question3/joinpathsconversion'
SELECT piwik_log_conversion.idvisit, piwik_log_conversion.idaction_url, piwik_paths_by_visit.paths FROM piwik_paths_by_visit JOIN piwik_log_conversion ON (piwik_paths_by_visit.idvisit =  piwik_log_conversion.idvisit);


