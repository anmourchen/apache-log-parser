package logparsing

import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

class Utils extends Serializable {
    val PATTERN = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)(.*)" (\d{3}) (\S+)""".r
    
    def containsTimeStamps(log: String):Boolean = {
        val res = PATTERN.findFirstMatchIn(log);
        return !res.isEmpty
    }
    
    // parse URL from the log
    def parseURL(log: String):(String) = {
        val res = PATTERN.findFirstMatchIn(log);
        val m = res.get
        return (m.group(6))
    }
    
    // parse Timestamps from the log
    def parseTimeStamps(log: String):(String) = {
        val res = PATTERN.findFirstMatchIn(log);
        val m = res.get
        return (m.group(4))
    }
    
    // parse HTTP code from the log
    def parseHTTP(log: String):(Int) = {
        val res = PATTERN.findFirstMatchIn(log);
        val m = res.get
        return (m.group(8).toInt)
    }

    // get the top 10 requested URLs
    def gettop10url(accessLogs:RDD[String], sc:SparkContext):Array[(String,Int)] = {
        val urlAccessLogs = accessLogs.filter(containsTimeStamps)
        var parsedURL = urlAccessLogs.map(parseURL)
        var url_tuples = parsedURL.map((_, 1));
        var frequencies = url_tuples.reduceByKey(_ + _);
        var sortedfrequencies = frequencies.sortBy(x => x._2, false)
        return sortedfrequencies.take(10)
    }
    
    // get top 5 time frames for highest/least traffic
    // asc = true: least traffic; asc = false: highest traffic
    def gettop5timeframe(accessLogs:RDD[String], sc:SparkContext, asc:Boolean):Array[(String,Int)] = {
        val urlAccessLogs = accessLogs.filter(containsTimeStamps)
        var parsedTimeFrame = urlAccessLogs.map(parseTimeStamps).map(_.substring(0, 14))
        var tf_tuples = parsedTimeFrame.map((_, 1));
        var frequencies = tf_tuples.reduceByKey(_ + _);
        var sortedfrequencies = frequencies.sortBy(x => x._2, asc)
        return sortedfrequencies.take(5)
    }
    
    // find HTTP code
    def countHTTP(accessLogs:RDD[String], sc:SparkContext):Array[(Int,Int)] = {
        val urlAccessLogs = accessLogs.filter(containsTimeStamps)
        var parsedHTTP = urlAccessLogs.map(parseHTTP)
        var http_tuples = parsedHTTP.map((_, 1));
        var frequencies = http_tuples.reduceByKey(_ + _);
        var sortedfrequencies = frequencies.sortBy(x => x._2, false)
        return sortedfrequencies.take(10)
    }
}

object EntryPoint {
    val usage = """
        Usage: EntryPoint <file_or_directory_in_hdfs>
        Eample: EntryPoint /data/spark/project/NASA_access_log_Aug95.gz
    """
    
    def main(args: Array[String]) {
        
        if (args.length != 2) {
            println("Expected:2 , Provided: " + args.length)
            println(usage)
            return;
        }

        var utils = new Utils

        // Create a local StreamingContext with batch interval of 10 second
        val conf = new SparkConf().setAppName("ParseLogs")
        val sc = new SparkContext(conf);
        sc.setLogLevel("WARN")

        var accessLogs = sc.textFile(args(1))
        val top10url = utils.gettop10url(accessLogs, sc)
        println("===== URL Count =====")
        for(i <- top10url){
            println(i)
        }
        
        val top5timeframe = utils.gettop5timeframe(accessLogs, sc, false)
        println("===== TOP 5 Time Frames for Highest Traffic =====")
        for(i <- top5timeframe){
            println(i)
        }
        
        val last5timeframe = utils.gettop5timeframe(accessLogs, sc, true)
        println("===== Top 5 Time Frames for Least Traffic =====")
        for(i <- last5timeframe){
            println(i)
        }
        
        val httpcodesCount = utils.countHTTP(accessLogs, sc)
        println("====== HTTP Code Count =======")
        for(i <- httpcodesCount){
            println(i)
        }
        
    }
}