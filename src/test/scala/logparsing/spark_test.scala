package logparsing

import org.scalatest.FunSuite
import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}

class SampleTest extends FunSuite with SharedSparkContext {
    test("Computing top10 URL") {
        var line1 = "in24.inetnebr.com - - [01/Aug/1995:00:00:01 -0400] \"GET /shuttle/missions/sts-68/news/sts-68-mcc-05.txt HTTP/1.0\" 200 1839"
        var line2 = "uplherc.upl.com - - \"GET /images/ksclogo-medium.gif HTTP/1.0\" 304 0"

        val utils = new Utils

        val list = List(line1, line2)
        val rdd = sc.parallelize(list);

        assert(rdd.count === list.length)   

        val records = utils.gettop10url(rdd, sc)
        assert(records.length === 1)    
        assert(records(0)._1 == "/shuttle/missions/sts-68/news/sts-68-mcc-05.txt")
    }
    test("Computing top5 time frames by traffic") {
        var line1 = "in24.inetnebr.com - - [01/Aug/1995:00:00:01 -0400] \"GET /shuttle/missions/sts-68/news/sts-68-mcc-05.txt HTTP/1.0\" 200 1839"
        var line2 = "uplherc.upl.com - - \"GET /images/ksclogo-medium.gif HTTP/1.0\" 304 0"
        
        val list = List(line1, line2)
        val rdd = sc.parallelize(list);

        val utils = new Utils
        val records = utils.gettop5timeframe(rdd, sc, false)
        assert(records.length === 1)
        assert(records(0)._1 == "01/Aug/1995:00")
    }
    test("Count HTTP codes") {
        var line1 = "in24.inetnebr.com - - [01/Aug/1995:00:00:01 -0400] \"GET /shuttle/missions/sts-68/news/sts-68-mcc-05.txt HTTP/1.0\" 200 1839"
        var line2 = "uplherc.upl.com - - \"GET /images/ksclogo-medium.gif HTTP/1.0\" 304 0"
        
        val list = List(line1, line2)
        val rdd = sc.parallelize(list);

        val utils = new Utils
        val records = utils.countHTTP(rdd, sc)
        assert(records.length === 1)
        assert(records(0)._1 == 200)
    }
}