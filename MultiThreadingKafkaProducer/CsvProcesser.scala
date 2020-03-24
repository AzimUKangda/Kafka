import scala.io.Source

class CsvProcessor extends CsvReader {

  override def readCsv(dataFile: String): List[Data] = {
    for {
      line <- Source.fromFile(dataFile).getLines().drop(1).toList
      values = line.split(",").map(_.trim)
    } yield
      Data(
        values(0),
        values(1),
        values(2),
        values(3).toDouble,
        values(4).toDouble,
        values(5).toDouble,
        values(6).toDouble,
        values(7).toDouble,
        values(8).toDouble,
        values(9).toDouble,
        values(10),
        values(11).toDouble,
        values(12)
      )
  }
}
