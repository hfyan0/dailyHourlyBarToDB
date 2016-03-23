import org.nirvana._
import org.joda.time.{Period, DateTime, LocalDate, Duration, Days}

object CorpActionAdj {
  def LoadConfigFile(filepath: String): Map[String, (Double, Double)] = {
    var resMap = Map[String, (Double, Double)]()

    val lines = scala.io.Source.fromFile(filepath).getLines.toList

    lines.foreach(l => {
      val csv = l.split(",").toList
      val ldcsv = csv(0).split("-").toList
      val ld = new LocalDate(ldcsv(0).toInt, ldcsv(1).toInt, ldcsv(2).toInt)
      val ldnow = new LocalDate()

      if (Days.daysBetween(ldnow, ld).getDays() == 1) {
        resMap += csv(1) -> (csv(2).toDouble, csv(3).toDouble)
      }

    })

    resMap
  }
  def applyCorpActAdj(lsohlcb: List[OHLCBar], adjmap: Map[String, (Double, Double)]): List[OHLCBar] = {

    lsohlcb.map(ohlcb => {
      val symbol = ohlcb.symbol

      if (adjmap.contains(symbol)) {
        val (deltaadj, ratioadj) = adjmap(symbol)
        if (deltaadj == 0.0 && ratioadj != 1.0) {
          OHLCBar(
            ohlcb.dt,
            symbol,
            OHLCPriceBar(
              ohlcb.priceBar.o / ratioadj,
              ohlcb.priceBar.h / ratioadj,
              ohlcb.priceBar.l / ratioadj,
              ohlcb.priceBar.c / ratioadj,
              (ohlcb.priceBar.v.toDouble * ratioadj).toLong
            )
          )
        }
        else if (deltaadj != 0.0 && ratioadj == 1.0) {
          OHLCBar(
            ohlcb.dt,
            symbol,
            OHLCPriceBar(
              ohlcb.priceBar.o - deltaadj,
              ohlcb.priceBar.h - deltaadj,
              ohlcb.priceBar.l - deltaadj,
              ohlcb.priceBar.c - deltaadj,
              ohlcb.priceBar.v
            )
          )
        }
        else ohlcb

      }
      else ohlcb
    })

  }

}

object DailyHourlyBarToDB {
  def main(args: Array[String]) =
    {
      println("DailyHourlyBarToDB starts")

      if (args.length == 0) {
        println("USAGE   java -jar ... [properties file] [d1 | h1 | m15] [corpactadj | nocorpactadj].")
        System.exit(1)
      }

      Config.readPropFile(args(0))
      val dataFreq = args(1)
      val bDoCorpActAdj = if (args(2) == "corpactadj") true else false;

      //--------------------------------------------------
      // load corp_action_adj
      //--------------------------------------------------
      val mapCorpActAdj = if (bDoCorpActAdj) CorpActionAdj.LoadConfigFile(Config.corpActionFile) else Map[String, (Double, Double)]()

      if (dataFreq == "h1") {
        //--------------------------------------------------
        // H1
        //--------------------------------------------------
        val lsH1Files = SUtil.getFilesInDir(Config.h1_ohlc_folder)
        println(Config.h1_ohlc_folder)
        // lsH1Files.foreach(println)

        lsH1Files.foreach(h1file =>
          {
            val lsdatalines = scala.io.Source.fromFile(h1file).getLines.toList
            val lsohlcb = lsdatalines.map(DataFmtAdaptors.parseBlmgFmt1(_, true)).filter(_ != None).map(_.get).takeRight(Config.h1_req_num)

            if (lsohlcb.length > 0) {
              DBProcessor.deleteMDHourlyTable(lsohlcb.head.symbol)

              val lsohlcb2 = if (bDoCorpActAdj) CorpActionAdj.applyCorpActAdj(lsohlcb, mapCorpActAdj) else lsohlcb
              DBProcessor.batchInsertHourlyMD(lsohlcb2)
            }
          })
      }

      else if (dataFreq == "d1") {
        // //--------------------------------------------------
        // // D1 (from folder with 1 symbol per file)
        // //--------------------------------------------------
        // val lsD1Files = SUtil.getFilesInDir(Config.d1_ohlc_folder)
        // println(Config.d1_ohlc_folder)
        // // lsD1Files.foreach(println)
        //
        // lsD1Files.foreach(d1file =>
        //   {
        //     val lsdatalines = scala.io.Source.fromFile(d1file).getLines.toList
        //     val lsohlcb = lsdatalines.map(DataFmtAdaptors.parseBlmgFmt1(_,true)).filter(_ != None).map(_.get).takeRight(Config.d1_req_num)
        //
        //     if (lsohlcb.length > 0) {
        //       DBProcessor.deleteMDDailyTable(lsohlcb.head.symbol)
        //       DBProcessor.batchInsertDailyMD(lsohlcb)
        //     }
        //   })

        //--------------------------------------------------
        // D1 (all in one file)
        //--------------------------------------------------
        val lsD1Lines = scala.io.Source.fromFile(Config.d1_ohlc_file).getLines.toList

        //--------------------------------------------------
        // too big memory usage
        //--------------------------------------------------
        // val mapsymlsohlcb = lsD1Lines.map(DataFmtAdaptors.parseBlmgFmt1(_, true)).filter(_ != None).map(_.get).groupBy(_.symbol)

        //--------------------------------------------------
        // alternative
        //--------------------------------------------------
        var mapsymlsohlcb = Map[String, List[OHLCBar]]()

        lsD1Lines.foreach(line => {

          DataFmtAdaptors.parseBlmgFmt1(line, true) match {
            case None => Unit
            case Some(ohlcb) => {
              val sym = ohlcb.symbol

              val orilsohlcb: List[OHLCBar] = mapsymlsohlcb.get(sym) match {
                case None    => List[OHLCBar]()
                case Some(x) => x
              }
              mapsymlsohlcb += sym -> (orilsohlcb :+ ohlcb)
            }
          }
        })
        //--------------------------------------------------

        mapsymlsohlcb.foreach {
          case (symbol, lsohlcb) => {
            if (lsohlcb.length > 0) {
              DBProcessor.deleteMDDailyTable(symbol)

              val lsohlcb2 = if (bDoCorpActAdj) CorpActionAdj.applyCorpActAdj(lsohlcb, mapCorpActAdj) else lsohlcb
              DBProcessor.batchInsertDailyMD(lsohlcb2.takeRight(Config.d1_req_num))
            }
          }
        }
      }

      else if (dataFreq == "m15") {
        //--------------------------------------------------
        // m15
        //--------------------------------------------------
        val lsM15Files = SUtil.getFilesInDir(Config.m15_ohlc_folder)
        println(Config.m15_ohlc_folder)

        lsM15Files.foreach(m15file =>
          {
            val lsdatalines = scala.io.Source.fromFile(m15file).getLines.toList
            val lsM15ohlcb = lsdatalines.map(DataFmtAdaptors.parseBlmgFmt1(_, true)).filter(_ != None).map(_.get).takeRight(Config.m15_req_num)

            var lsH1ohlcb = List[OHLCBar]()
            if (lsM15ohlcb.length > 0) {
              //--------------------------------------------------
              // aggregate the bars to hourly bars
              //--------------------------------------------------
              var lasthour = 2300
              var H1Open: Double = 0
              var H1High: Double = 0
              var H1Low: Double = 0
              var H1Close: Double = 0
              var H1Volume: Long = 0

              for (ohlcb <- lsM15ohlcb) {
                //--------------------------------------------------
                // reset on a new day
                //--------------------------------------------------
                if (lasthour > ohlcb.dt.getHourOfDay) {
                  H1Open = 0
                  H1High = 0
                  H1Low = 0
                  H1Close = 0
                  H1Volume = 0
                }
                lasthour = ohlcb.dt.getHourOfDay
                if (H1Open == 0) H1Open = ohlcb.priceBar.o
                if (H1High == 0 || ohlcb.priceBar.h > H1High) H1High = ohlcb.priceBar.h
                if (H1Low == 0 || ohlcb.priceBar.l < H1Low) H1Low = ohlcb.priceBar.l
                H1Volume += ohlcb.priceBar.v

                //--------------------------------------------------
                // give out a bar on every hour
                //--------------------------------------------------
                if (ohlcb.dt.getMinuteOfHour == 0) {
                  H1Close = ohlcb.priceBar.c
                  lsH1ohlcb ::= OHLCBar(ohlcb.dt, ohlcb.symbol, OHLCPriceBar(H1Open, H1High, H1Low, H1Close, H1Volume))
                  H1Open = 0
                  H1High = 0
                  H1Low = 0
                  H1Close = 0
                  H1Volume = 0
                }
              }

              DBProcessor.deleteMDHourlyTable(lsH1ohlcb.head.symbol)

              val lsohlcb2 = if (bDoCorpActAdj) CorpActionAdj.applyCorpActAdj(lsH1ohlcb, mapCorpActAdj) else lsH1ohlcb
              DBProcessor.batchInsertHourlyMD(lsohlcb2)
            }
          })
      }

      //--------------------------------------------------
      println("DailyHourlyBarToDB ended")
    }
}

