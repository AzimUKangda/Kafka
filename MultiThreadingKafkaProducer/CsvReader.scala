trait CsvReader {
  def readCsv(dataFile: String): List[Data]
}


case class Data(symbol: String, series: String, open: String, high: Double, low: Double, close: Double, last: Double, previousClose: Double, totalTradedQty: Double, totalTradedVal: Double, tradeDate: String,totalTrades: Double, isinCode: String ){

   def jsonString(): String ={
    s"""{"symbol":"$symbol","series":"$series","open":$open,"high":$high,"low":$low,"close":$close,"last":$last,"previousClose":$previousClose,"totalTradedQty":$totalTradedQty,"totalTradedVal":$totalTradedVal,"tradeDate":"$tradeDate","totalTrades":$totalTrades,"isinCode":"$isinCode"}"""
  }
}
