#Apache log parsing#

##Data set

Dataset is located in `/data/spark/project/NASA_access_log_Aug95.gz` directory in HDFS

Above dataset is access log of NASA Kennedy Space Center WWW server in Florida.

The logs are an ASCII file with one line per request, with the following columns:

* host - making the request. A hostname when possible, otherwise the Internet address if the name could not be looked up.
* timestamp - in the format "DAY MON DD HH:MM:SS YYYY", where DAY is the day of the week, MON is the name of the month, DD is the day of the month, HH:MM:SS is the time of day using a 24-hour clock, and YYYY is the year. The timezone is -0400.
* request URL - given in quotes.
* HTTP reply code.
* Bytes returned by the server.
* Note that from `01/Aug/1995:14:52:01` until `03/Aug/1995:04:36:13` there are no accesses recorded, as the Web server was shut down, due to Hurricane Erin.

## Get Top 10 requested URLs

Write spark code to find out top 10 requested URLs along with a count of the number of times they have been requested (This information will help the company to find out most popular pages and how frequently they are accessed).
```
(/images/NASA-logosmall.gif,97410)
(/images/KSC-logosmall.gif,75337)
(/images/MOSAIC-logosmall.gif,67448)
(/images/USA-logosmall.gif,67068)
(/images/WORLD-logosmall.gif,66444)
(/images/ksclogo-medium.gif,62778)
(/ksc.html,43687)
(/history/apollo/images/apollo-logo1.gif,37826)
(/images/launch-logo.gif,35138)
(/,30347)
```

## Get Top 5 time frames for high traffic

Write spark code to find out the top five-time frame for high traffic (which day of the week or hour of the day receives peak traffic, this information will help the company to manage resources for handling peak traffic load).

```
(31/Aug/1995:11,6319)
(31/Aug/1995:10,6283)
(31/Aug/1995:13,5948)
(30/Aug/1995:15,5919)
(31/Aug/1995:09,5627)
```

## Get Top 5 time frames for least traffic

Write spark code to find out top 5 time frames of least traffic (which day of the week or hour of the day receives least traffic, this information will help the company to do production deployment in that time frame so that less number of users will be affected if something goes wrong during deployment)

```
(03/Aug/1995:04,16)
(03/Aug/1995:09,22)
(03/Aug/1995:05,43)
(03/Aug/1995:10,57)
(03/Aug/1995:07,58)
```

## Find HTTP codes

Write Spark code to find out unique HTTP codes returned by the server along with count (this information is helpful for DevOps team to find out how many requests are failing so that appropriate action can be taken to fix the issue)

```
(200,1398988)
(304,134146)
(302,26444)
(404,10056)
(403,171)
(501,27)
(500,3)
```
