import org.nirvana._
import org.joda.time.{Period, DateTime, LocalDate, Duration, Days}

object CorpActionAdj {

  def LoadCorpActForParticularDate(filepath: String, nextTradingDate: DateTime): Map[String, (Double, Double)] = {
    var resMap = Map[String, (Double, Double)]()

    val lines = scala.io.Source.fromFile(filepath).getLines.toList

    lines.foreach(l => {
      val csv = l.split(",").toList
      val ldcsv = csv(0).split("-").toList
      val ld = new DateTime(ldcsv(0).toInt, ldcsv(1).toInt, ldcsv(2).toInt, 0, 0, 0)

      if (nextTradingDate.equals(ld)) {
        resMap += csv(1) -> (csv(2).toDouble, csv(3).toDouble)
      }

    })

    resMap
  }

  def LoadCorpActRatio(filepath: String): Map[String, List[(DateTime, Double)]] = {

    val lines = scala.io.Source.fromFile(filepath).getLines.toList

    val lsTup = lines.map(l => {
      val csv = l.split(",").toList
      (Config.dfmt.parseDateTime(csv(0)).withTime(23, 59, 59, 0), csv(1), csv(2).toDouble)
    })

    lsTup.groupBy(_._2).map { case (sym, lstup) => { (sym, lstup.map(x => (x._1, x._3)).sortBy(_._1.getMillis)) } }.toMap

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

  def applyCorpActAdjRatio(lsohlcb: List[OHLCBar], adjmap: Map[String, List[(DateTime, Double)]]): List[OHLCBar] = {

    def multiplyWithRndg(a: Double, b: Double): Double = {
      val scale = 100000.0
      Math.round(a * b * scale) / scale
    }

    lsohlcb.groupBy(_.symbol).map {
      case (sym, lsohlcbpersym) => {

        val newlsohlcbpersym =
          if (adjmap.contains(sym)) {
            val lsadjdtratio = adjmap.get(sym).get
            lsohlcbpersym.map(ohlcb => {

              val lsCurOrLater = lsadjdtratio.filter(_._1.getMillis >= ohlcb.dt.getMillis)
              val ratioToApply = if (!lsCurOrLater.isEmpty) lsCurOrLater.head._2 else lsadjdtratio.last._2

              ohlcb.copy(priceBar = OHLCPriceBar(
                multiplyWithRndg(ohlcb.priceBar.o, ratioToApply),
                multiplyWithRndg(ohlcb.priceBar.h, ratioToApply),
                multiplyWithRndg(ohlcb.priceBar.l, ratioToApply),
                multiplyWithRndg(ohlcb.priceBar.c, ratioToApply),
                ohlcb.priceBar.v
              ))

            })
          }
          else {
            lsohlcbpersym
          }

        newlsohlcbpersym

      }
    }.toList.flatten

  }

}

object DailyHourlyBarToDB {
  def main(args: Array[String]) =
    {
      println("DailyHourlyBarToDB starts")

      if (args.length == 0) {
        println("USAGE   java -jar ... [properties file] [d1 | h1 | m15]")
        System.exit(1)
      }

      Config.readPropFile(args(0))
      val dataFreq = args(1)
      val nextTradingDate = Config.dfmt.parseDateTime(args(2))

      //--------------------------------------------------
      // load corp_action_adj
      //--------------------------------------------------
      val mapCorpActAdj = if (Config.adjByCorpAct) CorpActionAdj.LoadCorpActForParticularDate(Config.corpActionFile, nextTradingDate) else Map[String, (Double, Double)]()
      val mapCorpActAdjRatio = if (Config.adjByCorpActRatio) CorpActionAdj.LoadCorpActRatio(Config.corpActionRatioFile) else Map[String, List[(DateTime, Double)]]()

      println("finished loading mapCorpActAdjRatio")
      if (Config.adjByCorpAct) println(mapCorpActAdj)
      //--------------------------------------------------

      //--------------------------------------------------
      if (dataFreq == "test") {
        println(mapCorpActAdj)
        println(mapCorpActAdjRatio)
        System.exit(0)
      }

      else if (dataFreq == "d1") {
        // //--------------------------------------------------
        // // D1 (from folder with 1 symbol per file)
        // //--------------------------------------------------
        // val lsD1Files = SUtil.getFilesInDir(Config.d1_ohlc_folder)
        // println("chkpt1")
        // // lsD1Files.foreach(println)
        //
        // lsD1Files.foreach(d1file => {
        //     val lsdatalines = scala.io.Source.fromFile(d1file).getLines.toList
        //     val lsohlcb_tmp = lsdatalines.map(DataFmtAdaptors.parseBlmgFmt1(_,true)).filter(_ != None).map(_.get)
        //     val lsohlcb = if (Config.onlyInsertTheLatestBars) lsohlcb_tmp.takeRight(Config.d1_req_num) else lsohlcb_tmp
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

          val parseFx = if (Config.source_ohlc_format == "blmg") DataFmtAdaptors.parseBlmgFmt1 _ else DataFmtAdaptors.parseCashOHLCFmt1 _

          parseFx(line, true) match {
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

              val lsohlcb2 = if (Config.adjByCorpAct) CorpActionAdj.applyCorpActAdj(lsohlcb, mapCorpActAdj) else lsohlcb
              // val lsohlcb3 = if (Config.adjByCorpActRatio) CorpActionAdj.applyCorpActAdjRatio(lsohlcb2, mapCorpActAdjRatio) else lsohlcb2
              val lsohlcb3 = lsohlcb2
              println("The program won't adjust the daily price with the corporate action adjustment ratio for you. please use the Bloomberg adjusted prices directly")
              DBProcessor.batchInsertDailyMD(lsohlcb3)
            }
          }
        }
      }

      else if (dataFreq == "m15") {
        //--------------------------------------------------
        // m15: so much code because need to aggregate hourly bars from 15 min bars
        //--------------------------------------------------
        val lsM15Files = SUtil.getFilesInDir(Config.m15_ohlc_folder)
        println("chkpt1")

        lsM15Files.foreach(m15file => {
          val lsdatalines = scala.io.Source.fromFile(m15file).getLines.toList

          val parseFx = if (Config.source_ohlc_format == "blmg") DataFmtAdaptors.parseBlmgFmt1 _ else DataFmtAdaptors.parseCashOHLCFmt1 _

          val lsM15ohlcb_tmp = lsdatalines.map(parseFx(_, true)).filter(_ != None).map(_.get)
          val lsM15ohlcb = if (Config.onlyInsertTheLatestBars) lsM15ohlcb_tmp.takeRight(Config.m15_req_num) else lsM15ohlcb_tmp

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

            val lsohlcb2 = if (Config.adjByCorpAct) CorpActionAdj.applyCorpActAdj(lsH1ohlcb, mapCorpActAdj) else lsH1ohlcb
            // val lsohlcb3 = if (Config.adjByCorpActRatio) CorpActionAdj.applyCorpActAdjRatio(lsohlcb2, mapCorpActAdjRatio) else lsohlcb2
            val lsohlcb3 = lsohlcb2
            println("The program won't adjust the hourly ohlc (converted from m15) with the corporate action adjustment ratio for you. This is not a valid use case.")
            DBProcessor.batchInsertHourlyMD(lsohlcb3)
          }
        })
      }
      else if (dataFreq == "h1") {
        //--------------------------------------------------
        // H1
        // assume 1 symbol per file
        //--------------------------------------------------
        val lsH1Files = SUtil.getFilesInDir(Config.h1_ohlc_folder)
        println("chkpt1")

        lsH1Files.foreach(h1file => {
          println(h1file)
          val lsdatalines = scala.io.Source.fromFile(h1file).getLines.toList

          val parseFx = if (Config.source_ohlc_format == "blmg") DataFmtAdaptors.parseBlmgFmt1 _ else DataFmtAdaptors.parseCashOHLCFmt1 _

          val lsohlcb_tmp = lsdatalines.map(parseFx(_, true)).filter(_ != None).map(_.get)
          val lsohlcb = if (Config.onlyInsertTheLatestBars) lsohlcb_tmp.takeRight(Config.h1_req_num) else lsohlcb_tmp

          if (lsohlcb.length > 0) {
            DBProcessor.deleteMDHourlyTable(lsohlcb.head.symbol)

            val lsohlcb2 = if (Config.adjByCorpAct) CorpActionAdj.applyCorpActAdj(lsohlcb, mapCorpActAdj) else lsohlcb
            val lsohlcb3 = if (Config.adjByCorpActRatio) CorpActionAdj.applyCorpActAdjRatio(lsohlcb2, mapCorpActAdjRatio) else lsohlcb2
            DBProcessor.batchInsertHourlyMD(lsohlcb3)
          }
        })
      }

      //--------------------------------------------------
      println("DailyHourlyBarToDB ended")
    }
}

