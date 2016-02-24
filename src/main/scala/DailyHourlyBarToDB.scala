import org.nirvana._

object DailyHourlyBarToDB {
  def main(args: Array[String]) =
    {
      println("DailyHourlyBarToDB starts")

      if (args.length == 0) {
        println("not enough arguments.")
        println("first argument is the properties file.")
        System.exit(1)
      }

      Config.readPropFile(args(0))

      //--------------------------------------------------
      // H1
      //--------------------------------------------------
      val lsH1Files = SUtil.getFilesInDir(Config.h1_ohlc_folder)
      println(Config.h1_ohlc_folder)
      // lsH1Files.foreach(println)

      lsH1Files.foreach(h1file =>
        {
          val lsdatalines = scala.io.Source.fromFile(h1file).getLines.toList
          val lsohlcb = lsdatalines.map(DataFmtAdaptors.parseBlmgFmt1(_,true)).filter(_ != None).map(_.get).takeRight(Config.h1_req_num)

          if (lsohlcb.length > 0) {
            DBProcessor.deleteMDHourlyTable(lsohlcb.head.symbol)
            DBProcessor.batchInsertHourlyMD(lsohlcb)
          }
        })

      //--------------------------------------------------
      // D1
      //--------------------------------------------------
      val lsD1Files = SUtil.getFilesInDir(Config.d1_ohlc_folder)
      println(Config.d1_ohlc_folder)
      // lsD1Files.foreach(println)

      lsD1Files.foreach(d1file =>
        {
          val lsdatalines = scala.io.Source.fromFile(d1file).getLines.toList
          val lsohlcb = lsdatalines.map(DataFmtAdaptors.parseBlmgFmt1(_,true)).filter(_ != None).map(_.get).takeRight(Config.d1_req_num)

          if (lsohlcb.length > 0) {
            DBProcessor.deleteMDDailyTable(lsohlcb.head.symbol)
            DBProcessor.batchInsertDailyMD(lsohlcb)
          }
        })

      //--------------------------------------------------
      println("DailyHourlyBarToDB ended")
    }
}
