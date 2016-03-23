import org.scalatest.junit.AssertionsForJUnit
import scala.collection.mutable.ListBuffer
import org.junit.Assert._
import org.junit.Test
import org.junit.Before
import org.joda.time.{DateTime, LocalTime}
import org.nirvana._

class DailyHourlyBarToDBTest extends AssertionsForJUnit {

  @Test def testApplyCorpActAdj() {

    var m = Map[String, (Double, Double)]()
    m += "ACTIONRATIO" -> (0, 2)
    m += "ACTIONDELTA" -> (0.5, 1)

    var lsohlcb = List[OHLCBar]()
    val justsomedatetime = new DateTime()
    lsohlcb ::= OHLCBar(justsomedatetime, "NOACTION", OHLCPriceBar(1, 2, 3, 4, 5))
    lsohlcb ::= OHLCBar(justsomedatetime, "ACTIONRATIO", OHLCPriceBar(1, 2, 3, 4, 5))
    lsohlcb ::= OHLCBar(justsomedatetime, "ACTIONDELTA", OHLCPriceBar(1, 2, 3, 4, 5))

    val res = CorpActionAdj.applyCorpActAdj(lsohlcb, m)

    assertEquals(res(0), OHLCBar(justsomedatetime, "ACTIONDELTA", OHLCPriceBar(0.5, 1.5, 2.5, 3.5, 5)))
    assertEquals(res(1), OHLCBar(justsomedatetime, "ACTIONRATIO", OHLCPriceBar(0.5, 1, 1.5, 2, 10)))
    assertEquals(res(2), OHLCBar(justsomedatetime, "NOACTION", OHLCPriceBar(1, 2, 3, 4, 5)))

  }

}
