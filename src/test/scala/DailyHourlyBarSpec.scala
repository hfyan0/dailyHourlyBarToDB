import org.scalatest.junit.AssertionsForJUnit
import scala.collection.mutable.ListBuffer
import org.junit.Assert._
import org.junit.Test
import org.junit.Before
import org.joda.time.{Period, DateTime, LocalDate, Duration, Days}
import org.nirvana._

class DailyHourlyBarToDBTest extends AssertionsForJUnit {

  @Test def testApplyCorpActAdj() {

    var m = Map[String, (Double, Double)]()
    m += "ACTIONRATIO" -> (0, 2)
    m += "ACTIONDELTA" -> (0.5, 1)

    var lsohlcb = List[OHLCBar]()
    val dtNow = new DateTime()

    val ohlcpb = OHLCPriceBar(1, 2, 3, 4, 5)
    lsohlcb ::= OHLCBar(dtNow, "NOACTION", ohlcpb)
    lsohlcb ::= OHLCBar(dtNow, "ACTIONRATIO", ohlcpb)
    lsohlcb ::= OHLCBar(dtNow, "ACTIONDELTA", ohlcpb)

    val res = CorpActionAdj.applyCorpActAdj(lsohlcb, m)

    assertEquals(res(0), OHLCBar(dtNow, "ACTIONDELTA", OHLCPriceBar(0.5, 1.5, 2.5, 3.5, 5)))
    assertEquals(res(1), OHLCBar(dtNow, "ACTIONRATIO", OHLCPriceBar(0.5, 1, 1.5, 2, 10)))
    assertEquals(res(2), OHLCBar(dtNow, "NOACTION", OHLCPriceBar(1, 2, 3, 4, 5)))
  }

  @Test def testApplyCorpActAdjRatio() {

    var lsohlcb = List[OHLCBar]()
    val dtNow = new DateTime().withTime(0, 0, 0, 0)
    val dtYesterday = new DateTime().plusDays(-1).withTime(0, 0, 0, 0)
    val dt19900101 = new DateTime(1990, 1, 1, 0, 0, 0)

    var m = Map[String, List[(DateTime, Double)]]()
    m += "DUMMY1" -> List((dtNow, 0.2),(dt19900101, 4))
    m += "DUMMY2" -> List((dtNow, 0.5))
    m += "DUMMY3" -> List((dt19900101, 1.2))

    val ohlcpb = OHLCPriceBar(1, 2, 3, 4, 5)
    lsohlcb ::= OHLCBar(dtYesterday, "DUMMY1", ohlcpb)
    lsohlcb ::= OHLCBar(dtYesterday, "DUMMY2", ohlcpb)
    lsohlcb ::= OHLCBar(dtYesterday, "DUMMY3", ohlcpb)

    val res = CorpActionAdj.applyCorpActAdjRatio(lsohlcb, m)

    assertEquals(res(0), OHLCBar(dtYesterday, "DUMMY3", OHLCPriceBar(1.2, 2.4, 3.6, 4.8, 5)))
    assertEquals(res(1), OHLCBar(dtYesterday, "DUMMY2", OHLCPriceBar(0.5, 1.0, 1.5, 2.0, 5)))
    assertEquals(res(2), OHLCBar(dtYesterday, "DUMMY1", OHLCPriceBar(0.2, 0.4, 0.6, 0.8, 5)))
  }

}
