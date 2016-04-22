import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.FileInputStream;
import java.util.Date;
import java.util.Properties;
import org.joda.time.{DateTime, LocalTime}
import org.joda.time.format.DateTimeFormat

object Config {

  val dfmt = DateTimeFormat.forPattern("yyyy-MM-dd")

  def readPropFile(propFileName: String) {

    try {

      val prop = new Properties()
      prop.load(new FileInputStream(propFileName))

      (1 to 9).foreach(i => {
        val connStr = prop.getProperty("jdbcConnStr" + i)
        if (connStr != "" && connStr != null) {
          jdbcConnStr ::= connStr
          jdbcUser ::= prop.getProperty("jdbcUser" + i)
          jdbcPwd ::= prop.getProperty("jdbcPwd" + i)
        }
      })

      source_ohlc_format = prop.getProperty("source_ohlc_format")
      h1_ohlc_folder = prop.getProperty("h1_ohlc_folder")
      m15_ohlc_folder = prop.getProperty("m15_ohlc_folder")
      d1_ohlc_file = prop.getProperty("d1_ohlc_file")
      adjByCorpAct = prop.getProperty("adjByCorpAct").toBoolean
      corpActionFile = prop.getProperty("corpActionFile")
      adjByCorpActRatio = prop.getProperty("adjByCorpActRatio").toBoolean
      corpActionRatioFile = prop.getProperty("corpActionRatioFile")
      onlyInsertTheLatestBars = prop.getProperty("onlyInsertTheLatestBars").toBoolean

      jdbcConnStr.foreach(println)
      jdbcUser.foreach(println)
      jdbcPwd.foreach(println)

      println(source_ohlc_format)
      println(h1_ohlc_folder)
      println(m15_ohlc_folder)
      println(d1_ohlc_file)
      println(adjByCorpAct)
      println(corpActionFile)
      println(adjByCorpActRatio)
      println(corpActionRatioFile)
      println(onlyInsertTheLatestBars)

    }
    catch {
      case e: Exception =>
        {
          e.printStackTrace()
          sys.exit(1)
        }
    }
  }

  //--------------------------------------------------
  // JDBC
  //--------------------------------------------------
  var jdbcConnStr = List[String]()
  var jdbcUser = List[String]()
  var jdbcPwd = List[String]()

  //--------------------------------------------------
  // data
  //--------------------------------------------------
  var source_ohlc_format = "blmg"
  var h1_ohlc_folder = ""
  var m15_ohlc_folder = ""
  var d1_ohlc_file = ""

  //--------------------------------------------------
  // truncate data because maneki does not need that many
  //--------------------------------------------------
  var h1_req_num = 200
  var m15_req_num = 800
  var d1_req_num = 120

  //--------------------------------------------------
  // corp action adjustment
  //--------------------------------------------------
  var adjByCorpAct = false
  var corpActionFile = ""

  //--------------------------------------------------
  // corp action ratio adjustment
  //--------------------------------------------------
  var adjByCorpActRatio = false
  var corpActionRatioFile = ""

  //--------------------------------------------------
  // req for maneki live or backtest are different
  //--------------------------------------------------
  var onlyInsertTheLatestBars = true

}
