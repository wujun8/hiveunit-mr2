add JAR etl-udf-1.0.0-SNAPSHOT.jar;
create temporary function url_ext_filter as 'cn.com.tontron.url.etl.udf.ExtFilterUDF';

CREATE TABLE url (url STRING, time_s STRING, ref STRING, agent STRING)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';
LOAD DATA LOCAL INPATH '${TABLE_DATA}' OVERWRITE INTO TABLE url;

SELECT t.url, concat_ws('\t', collect_set(t.agent)), COUNT(t.url) FROM url t WHERE url_ext_filter(t.url) GROUP BY t.url;
