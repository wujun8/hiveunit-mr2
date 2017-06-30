add JAR etludf.jar;
create temporary function url_ext_filter as 'cn.com.tontron.url.etl.udf.ExtFilterUDF';

CREATE TABLE url (url STRING, time_s STRING, ref STRING, agent STRING)
  PARTITIONED BY (dt STRING)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';
LOAD DATA LOCAL INPATH '${TABLE_DATA}' OVERWRITE INTO TABLE url PARTITION (dt='20170101');
LOAD DATA LOCAL INPATH '${TABLE_DATA2}' OVERWRITE INTO TABLE url PARTITION (dt='20170102');

SELECT * FROM url t WHERE url_ext_filter(t.url);
