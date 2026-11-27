CREATE EXTERNAL TABLE IF NOT EXISTS `stedi_db`.`step_trainer_landing` (
  `sensorReadingTime` float,
  `serialNumber` string,
  `distanceFromObject` int,
  `timestamp` bigint
) COMMENT "step_trainer_landing"
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://s3spspark/step_trainer/landing/'
TBLPROPERTIES ('classification' = 'json');
