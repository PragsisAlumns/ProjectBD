#SQOOP SCRIPT TO IMPORT THE PIWIK USEFULL TABLES.

#Example:
#sqoop import --connect jdbc:mysql://dev.loudacre.com/test --username training --password training --table <table> --target-dir /piwik/<table> --null-non-string '\\N'

#Example to list tables:
#sqoop list-tables --connect jdbc:mysql://dev.loudacre.com/test --username training --password training

#Example to create hive tables directely from mysql tables
#sqoop create-hive-table --connect jdbc:mysql://dev.loudacre.com/test --username training --password training --table piwik_log_action --fields-terminated-by ','


#IMPORTING TABLES.

sqoop import --connect jdbc:mysql://dev.loudacre.com/test --username training --password training \
--query 'SELECT idvisit, idvisitor, idaction_url, idaction_url_ref FROM piwik_log_link_visit_action WHERE $CONDITIONS' \
--split-by idvisit \
--target-dir /piwik/log_link_visit_action \
--null-non-string '\\N'

sqoop import --connect jdbc:mysql://dev.loudacre.com/test --username training --password training \
--query 'SELECT idgoal, pattern FROM piwik_goal WHERE $CONDITIONS' \
--split-by idgoal \
--target-dir /piwik/goal \
--null-non-string '\\N'

sqoop import --connect jdbc:mysql://dev.loudacre.com/test --username training --password training \
--query 'SELECT idaction, name FROM piwik_log_action WHERE $CONDITIONS' \
--split-by idaction \
--target-dir /piwik/log_action \
--null-non-string '\\N'

sqoop import --connect jdbc:mysql://dev.loudacre.com/test --username training --password training \
--query 'SELECT idvisit, idvisitor, idaction_url, url FROM piwik_log_conversion WHERE $CONDITIONS' \
--split-by idvisit \
--target-dir /piwik/log_conversion \
--null-non-string '\\N'






