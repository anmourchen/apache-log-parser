package logparsing

import org.scalatest.FlatSpec

class LogParserSpec extends FlatSpec {

  "parseURL" should "Extract URL" in {
    val utils = new Utils
    var line = "in24.inetnebr.com - - [01/Aug/1995:00:00:01 -0400] \"GET /shuttle/missions/sts-68/news/sts-68-mcc-05.txt HTTP/1.0\" 200 1839"
    var url = utils.parseURL(line)
    assert(url == "/shuttle/missions/sts-68/news/sts-68-mcc-05.txt")
  }
  
  "parseTimeStamps" should "Extract TimeStamp" in {
    val utils = new Utils
    var line = "in24.inetnebr.com - - [01/Aug/1995:00:00:01 -0400] \"GET /shuttle/missions/sts-68/news/sts-68-mcc-05.txt HTTP/1.0\" 200 1839"
    var timestamp = utils.parseTimeStamps(line)
    assert(timestamp == "01/Aug/1995:00:00:01 -0400")
  }
  
  "parseHTTP" should "Extract HTTP code" in {
    val utils = new Utils
    var line = "in24.inetnebr.com - - [01/Aug/1995:00:00:01 -0400] \"GET /shuttle/missions/sts-68/news/sts-68-mcc-05.txt HTTP/1.0\" 200 1839"
    var http = utils.parseHTTP(line)
    assert(http == 200)
  } 

  "containsURL" should "Check if URL exists in the log line" in {
    val utils = new Utils
    var line1 = "in24.inetnebr.com - - [01/Aug/1995:00:00:01 -0400] \"GET /shuttle/missions/sts-68/news/sts-68-mcc-05.txt HTTP/1.0\" 200 1839"

    assert(utils.containsTimeStamps(line1))
    
    var line2 = "uplherc.upl.com - - \"GET HTTP/1.0\" 304 0"
    assert(!utils.containsTimeStamps(line2))
  }
}