# AmazonReviewsData
Analytics on Amazon Reviews Data

# Problem Statement
Create a data pipeline to extract data from the CSV/JSON files, and import it into a database/DFS in order to perform analysis

# Source Data
Ratings: http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/ratings_Movies_and_TV.csv

Metadata: http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/meta_Movies_and_TV.json.gz 

Details of the Dataset : http://jmcauley.ucsd.edu/data/amazon/links.html

# Solution
I approached this problem in two ways. 
1. Using Hive
2. Using Java

Let's discuss each approach. 

# Hive ways

Steps :
=======================
#Downloading through wget  
```
[pghosh@1134 tkaway]$ wget http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/ratings_Movies_and_TV.csv
[pghosh@1134 tkaway]$ ls -alrt | grep -i rating
-rw-r--r-- 1 pghosh inguser 187517953 Apr 27  2016 ratings_Movies_and_TV.csv
[pghosh@1134 tkaway]$ cat ratings_Movies_and_TV.csv | wc -l
4607047
```

#Transferring the file to hive cluster
```
[pghosh@1134 tkaway]$ scp ratings_Movies_and_TV.csv pghosh@mapr.dev.lab.com:/home/pghosh/
[pghosh@mapr.dev  ~]$ ls -alrt | grep -i rat
-rw-r--r-- 1 pghosh pghosh 187517953 Jun 13 11:15 ratmtv.csv
[pghosh@mapr.dev  ~]$ hadoop fs -put ratmtv.csv /test/takeaway/
[pghosh@mapr.dev  ~]$ hadoop fs -ls /test/takeaway/
Found 1 items
-rwxr-xr-x   3 abc def  187517953 2021-06-13 11:21 /test/takeaway/ratmtv.csv
[pghosh@mapr.dev  ~]$ hadoop fs -cat /test/takeaway/ratmtv.csv | wc -l 
4607047
```

#Creating Hive external table

```
CREATE EXTERNAL TABLE IF NOT EXISTS takeaway.testratmtv(custid string, movid string, rat decimal(10,1),rattime bigint) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION '/test/takeaway';

CREATE EXTERNAL TABLE IF NOT EXISTS takeaway.testratmtv_dd(custid string, movid string, rat decimal(10,1),ratdate date, ratmonth int, ratyear int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION '/test/takeaway1';

INSERT OVERWRITE TABLE takeaway.testratmtv_dd select custid, movid, rat, from_unixtime(rattime,'yyyy-MM-dd') as ratdate,month(from_unixtime(rattime)) as ratmonth, year(from_unixtime(rattime)) as ratyear from takeaway.testratmtv;
```

#Running the query to get the results of the solution

```
0: jdbc:hive2://map.dev.lab.com> select a.movid from (select count(rat) as cnt, sum(rat) as sum, avg(rat) as avgrat, custid, movid from testratmtv_dd where ratmonth=10 group by movid,custid order by avgrat desc limit 5) a;


+-------------+
|   a.movid   |
+-------------+
| 1558908382  |
| B003EYVXV4  |
| B005LAII58  |
| 6301410831  |
| B00009W0U4  |
+-------------+
5 rows selected (75.705 seconds)
```

```

0: jdbc:hive2://map.dev.lab.com> select a.movid from (select count(rat) as cnt, sum(rat) as sum, avg(rat) as avgrat, custid, movid from testratmtv_dd where ratmonth=10 group by movid,custid order by avgrat limit 5) a;

+-------------+
|   a.movid   |
+-------------+
| B00D6MB83W  |
| 0783225849  |
| B002BWP2IK  |
| B001J66JQS  |
| B008JFUO72  |
+-------------+
5 rows selected (75.574 seconds)
```

```
0: jdbc:hive2://map.dev.lab.com> select a.movid, a.avgrat, a.pre_avgrat,(a.avgrat-a.pre_avgrat) as avgdiff from (with avgrat_m as (select ratmonth,avg(rat) as avgrat,movid from testratmtv_dd where ratmonth=5 group by ratmonth,movid) select ratmonth, avgrat, movid, lag(avgrat,1) over(order by ratmonth) pre_avgrat from avgrat_m) a order by avgdiff desc limit 5;

+-------------+-----------+---------------+----------+
|   a.movid   | a.avgrat  | a.pre_avgrat  | avgdiff  |
+-------------+-----------+---------------+----------+
| 0131858033  | 5.00000   | 1.00000       | 4.00000  |
| B00KHB3GSK  | 5.00000   | 1.00000       | 4.00000  |
| 0740309498  | 5.00000   | 1.00000       | 4.00000  |
| 0615172083  | 5.00000   | 1.00000       | 4.00000  |
| 0310684390  | 5.00000   | 1.00000       | 4.00000  |
+-------------+-----------+---------------+----------+
5 rows selected (106.304 seconds)
```

