CREATE TABLE customer_trusted(
  serialNumber string, 
  shareWithPublicAsOfDate bigint, 
  birthday string, 
  registrationDate bigint, 
  shareWithResearchAsOfDate bigint, 
  customerName string, 
  email string, 
  lastUpdateDate bigint, 
  phone string, 
  shareWithFriendsAsOfDate bigint)
LOCATION
  's3://seans-stedi-lakehouse/customers/trusted/'
TBLPROPERTIES(
    'format'='parquet',
    'table_type'='ICEBERG'
)
