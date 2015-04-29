sqoop import --connect jdbc:mysql://dev.loudacre.com/test --username training --password training \
--query 'SELECT idvisit, idvisitor, idaction_url, url FROM piwik_log_conversion WHERE $CONDITIONS' \
--split-by idvisit \
--target-dir /piwik/log_conversion \
--null-non-string '\\N'
