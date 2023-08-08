import redshift_connector

conn = redshift_connector.connect(
    host='redshift-cluster-covid19-project.cue9psgygrpj.eu-west-1.redshift.amazonaws.com',
    database='dev',
    user='awsuser',
    password='Password2023'
    )

conn.autocommit = True

cursor = redshift_connector.Cursor = conn.cursor()

cursor.execute("""
CREATE TABLE "dimDate" (
"index" INTEGER,
  "fips" REAL,
  "date" TIMESTAMP,
  "year" INTEGER,
  "month" INTEGER,
  "day_of_week" INTEGER
)
""")

cursor.execute("""
CREATE TABLE "dimRegion" (
"index" INTEGER,
  "fips" REAL,
  "province_state" TEXT,
  "country_region" TEXT,
  "latitude" REAL,
  "longitude" REAL,
  "county" TEXT,
  "state" TEXT
)
""")


cursor.execute("""
CREATE TABLE "dimHospital" (
"index" INTEGER,
  "fips" REAL,
  "state_name" TEXT,
  "latitude" REAL,
  "longtitude" REAL,
  "hq_address" TEXT,
  "hospital_name" TEXT,
  "hospital_type" TEXT,
  "hq_city" TEXT,
  "hq_state" TEXT
)
""")


cursor.execute("""
CREATE TABLE "factCovid" (
"index" INTEGER,
  "fips" REAL,
  "province_state" TEXT,
  "country_region" TEXT,
  "confirmed" REAL,
  "deaths" REAL,
  "recovered" REAL,
  "active" REAL,
  "date" INTEGER,
  "positive" INTEGER,
  "negative" REAL,
  "hospitalizedcurrently" REAL,
  "hospitalized" REAL,
  "hospitalizeddischarged" REAL
)
""")


cursor.execute("""
copy factcovid from 's3://project-covid19-de-self-paced/covid data/output/factCovid.csv'
credentials 'aws_iam_role=arn:aws:iam::756809683296:role/redshift-s3-iamaccess'
delimiter ','
region 'eu-west-1'
IGNOREHEADER 1
""")


cursor.execute("""
copy dimdate from 's3://project-covid19-de-self-paced/covid data/output/dimDate.csv'
credentials 'aws_iam_role=arn:aws:iam::756809683296:role/redshift-s3-iamaccess'
delimiter ','
region 'eu-west-1'
MAXERROR 100000
IGNOREHEADER 1
""")


cursor.execute("""
copy dimhospital from 's3://project-covid19-de-self-paced/covid data/output/dimHospital.csv'
credentials 'aws_iam_role=arn:aws:iam::756809683296:role/redshift-s3-iamaccess'
delimiter ','
region 'eu-west-1'
MAXERROR 100000
IGNOREHEADER 1
""")


cursor.execute("""
copy dimregion from 's3://project-covid19-de-self-paced/covid data/output/dimRegion_1.csv'
credentials 'aws_iam_role=arn:aws:iam::756809683296:role/redshift-s3-iamaccess'
delimiter ','
region 'eu-west-1'
MAXERROR 100000
IGNOREHEADER 1
""")


cursor.execute("""
copy dimregion from 's3://project-covid19-de-self-paced/covid data/output/dimRegion_2.csv'
credentials 'aws_iam_role=arn:aws:iam::756809683296:role/redshift-s3-iamaccess'
delimiter ','
region 'eu-west-1'
MAXERROR 100000
IGNOREHEADER 1
""")
