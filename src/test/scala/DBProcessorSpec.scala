import org.scalatest.junit.AssertionsForJUnit
import scala.collection.mutable.ListBuffer
import org.junit.Assert._
import org.junit.Test
import org.junit.Before
import org.joda.time.{DateTime, LocalTime}
import org.nirvana._

class DBProcessorTest extends AssertionsForJUnit {

  @Test def testInsertDaily() {
    Config.readPropFile("/home/qy/Dropbox/nirvana/sbtProj/dailyHourlyBarToDB/132.properties")
    val ohlcpb = OHLCPriceBar(1, 4, 2, 3, 5000)
    val ohlcb = OHLCBar(new DateTime(2016, 2, 11, 16, 20, 45), "TESTSYM", ohlcpb)
    val lohlcb = ohlcb :: Nil
    DBProcessor.deleteMDDailyTable()
    DBProcessor.batchInsertDailyMD(lohlcb)
  }

  @Test def testInsertHourly() {
    Config.readPropFile("/home/qy/Dropbox/nirvana/sbtProj/dailyHourlyBarToDB/132.properties")
    val ohlcpb = OHLCPriceBar(1, 4, 2, 3, 5000)
    val ohlcb = OHLCBar(new DateTime(2016, 2, 11, 16, 20, 45), "TESTSYM", ohlcpb)
    val lohlcb = ohlcb :: Nil
    DBProcessor.deleteMDHourlyTable()
    DBProcessor.batchInsertHourlyMD(lohlcb)
  }

}
