import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord

class Dispatcher extends Runnable {

  val objectMapper: ObjectMapper = new ObjectMapper
  var dataToJsonNode: List[JsonNode] = List()

  var kafkaProducer: KafkaProducer[String, JsonNode] = _
  var topicName: String = _
  var dataFile: String = _
  val log: Logger = LogManager.getLogger

  def this(kafkaProducer: KafkaProducer[String, JsonNode],
           topicName: String,
           dataFile: String) {
    this
    this.kafkaProducer = kafkaProducer
    this.topicName = topicName
    this.dataFile = dataFile
  }

  override def run(): Unit = {
    var messageCounter: Int = 1
    val messageKey: String = dataFile.split('\\').last
    val currentProducer: String = Thread.currentThread.getName
    println(s"Starting Producer Thread $currentProducer")

    val csvProcessor: CsvProcessor = new CsvProcessor
    val data = csvProcessor.readCsv(dataFile)

    data.foreach { x =>
      val jsonNode: JsonNode = objectMapper.readTree(x.jsonString())
      dataToJsonNode = dataToJsonNode ++ List(jsonNode)
    }
    dataToJsonNode.foreach { data =>
      kafkaProducer.send(
        new ProducerRecord[String, JsonNode](topicName, messageKey, data))
      messageCounter = messageCounter + 1
    }
    println(s"sent $messageCounter records to Producer $currentProducer")
  }

}
