import org.nirvana._
import java.sql.{Connection, DriverManager, ResultSet, Timestamp};
import java.util.Properties;
import scala.collection.mutable.ListBuffer
import org.joda.time.{Period, DateTime, Duration}

object DBProcessor {

  //--------------------------------------------------
  // mysql
  //--------------------------------------------------
  Class.forName("com.mysql.jdbc.Driver")

  var lsConn = List[Connection]()

  (0 until Config.jdbcConnStr.length).foreach(i => {
    var p = new Properties()
    p.put("user", Config.jdbcUser(i))
    p.put("password", Config.jdbcPwd(i))
    val _conn = DriverManager.getConnection(Config.jdbcConnStr(i), p)
    lsConn ::= _conn
  })

  def deleteMDDailyTable() {
    lsConn.foreach(_conn => {
      try {
        val prep = _conn.prepareStatement("delete from market_data_daily_hk_stock")
        prep.executeUpdate
      }
    })
  }

  def deleteMDHourlyTable() {
    lsConn.foreach(_conn => {
      try {
        val prep = _conn.prepareStatement("delete from market_data_hourly_hk_stock")
        prep.executeUpdate
      }
    })
  }

  def deleteMDDailyTable(symbol: String) {
    lsConn.foreach(_conn => {
      try {
        val prep = _conn.prepareStatement("delete from market_data_daily_hk_stock where instrument_id = ?")
        prep.setString(1, symbol)
        prep.executeUpdate
      }
    })
  }

  def deleteMDHourlyTable(symbol: String) {
    lsConn.foreach(_conn => {
      try {
        val prep = _conn.prepareStatement("delete from market_data_hourly_hk_stock where instrument_id = ?")
        prep.setString(1, symbol)
        prep.executeUpdate
      }
    })
  }

  def batchInsertDailyMD(lohlcbar: List[OHLCBar]) {
    lsConn.foreach(_conn => {
      try {
        val prep = _conn.prepareStatement("insert into market_data_daily_hk_stock (timestamp,instrument_id,open,high,low,close,volume) values (?,?,?,?,?,?,?) ")

        lohlcbar.foreach {
          bar =>
            {
              prep.setString(1, SUtil.convertDateTimeToStr(bar.dt))
              prep.setString(2, bar.symbol)
              prep.setDouble(3, bar.priceBar.o)
              prep.setDouble(4, bar.priceBar.h)
              prep.setDouble(5, bar.priceBar.l)
              prep.setDouble(6, bar.priceBar.c)
              prep.setLong(7, bar.priceBar.v)
              prep.addBatch()
            }
        }
        prep.executeBatch
      }
    })

    //--------------------------------------------------
    // insert HSI price into daily_hsi_price if any
    //--------------------------------------------------
    val lHSIbars = lohlcbar.filter(_.symbol == "HSI")

    lsConn.foreach(_conn => {
      try {
        val prep = _conn.prepareStatement("insert into daily_hsi_price (timestamp,price) values (?,?) ")

        lHSIbars.foreach {
          bar =>
            {
              prep.setString(1, SUtil.convertDateTimeToStr(bar.dt))
              prep.setDouble(2, bar.priceBar.c)
              prep.addBatch()
            }
        }
        prep.executeBatch
      }
    })

  }

  def batchInsertHourlyMD(lohlcbar: List[OHLCBar]) {
    lsConn.foreach(_conn => {
      try {
        val prep = _conn.prepareStatement("insert into market_data_hourly_hk_stock (timestamp,instrument_id,open,high,low,close,volume) values (?,?,?,?,?,?,?) ")

        lohlcbar.foreach {
          bar =>
            {
              prep.setString(1, SUtil.convertDateTimeToStr(bar.dt))
              prep.setString(2, bar.symbol)
              prep.setDouble(3, bar.priceBar.o)
              prep.setDouble(4, bar.priceBar.h)
              prep.setDouble(5, bar.priceBar.l)
              prep.setDouble(6, bar.priceBar.c)
              prep.setLong(7, bar.priceBar.v)
              prep.addBatch()
            }
        }
        prep.executeBatch
      }
    })
  }

  def closeConn(): Unit = {
    lsConn.foreach(_.close)
  }
}

