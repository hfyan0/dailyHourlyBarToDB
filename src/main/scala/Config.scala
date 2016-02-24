import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.FileInputStream;
import java.util.Date;
import java.util.Properties;
import org.joda.time.{DateTime, LocalTime}

object Config {

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

      h1_ohlc_folder = prop.getProperty("h1_ohlc_folder")
      d1_ohlc_folder = prop.getProperty("d1_ohlc_folder")

      jdbcConnStr.foreach(println)
      jdbcUser.foreach(println)
      jdbcPwd.foreach(println)

      println(h1_ohlc_folder)
      println(d1_ohlc_folder)
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
  var h1_ohlc_folder = ""
  var d1_ohlc_folder = ""

  //--------------------------------------------------
  // truncate data because maneki does not need that many
  //--------------------------------------------------
  var h1_req_num = 200
  var d1_req_num = 120

}