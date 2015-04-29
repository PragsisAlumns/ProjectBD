sqoop import --connect jdbc:mysql://dev.loudacre.com/test --username training --password training \
--query 'SELECT idlink_va, idvisit, idaction_url, idaction_url_ref FROM piwik_log_link_visit_action WHERE $CONDITIONS' \
--split-by idlink_va \
--target-dir /piwik/log_link_visit_action \
--null-non-string '\\N'
