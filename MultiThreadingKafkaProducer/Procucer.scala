import com.fasterxml.jackson.databind.JsonNode
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import java.util.Properties
import java.io.IOException
import java.io.File
import java.util.concurrent.{ExecutorService, Executors}
import java.io.FileInputStream
import java.io.InputStream

object Producer {

  val log: Logger = LogManager.getLogger
  val kafkaProperties: String =
    getClass.getResource("/config/kafka.properties").getPath

  def main(args: Array[String]): Unit = {

    val basePath = args(0)
    val topicName = args(1)
    val numberOfThreads = args(2).toInt

    val executor: ExecutorService =
      Executors.newFixedThreadPool(numberOfThreads)
    val eventFiles: Array[String] =
      new File(basePath).listFiles.filter(_.isFile).map(file => file.toString)

    val properties: Properties = new Properties
    try {
      val kafkaConfig: InputStream = new FileInputStream(kafkaProperties)
      properties.load(kafkaConfig)
      properties.put("key.serializer",
                     "org.apache.kafka.common.serialization.StringSerializer")
      properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                     new JsonSerializer().getClass.getName)
    } catch {
      case ex: IOException => {
        log.error(s"Exception in reading Data File $ex")
        throw new RuntimeException(ex)
      }
    }
    val kafkaProducer: KafkaProducer[String, JsonNode] =
      new KafkaProducer[String, JsonNode](properties)

    try {
      eventFiles.foreach { file =>
        val dispatcher: Runnable =
          new Dispatcher(kafkaProducer, topicName, file)
        executor.execute(dispatcher)
      }
      executor.shutdown()
    } catch {
      case ex: Exception => {
        ex.printStackTrace
        kafkaProducer.close
        throw new RuntimeException(ex)
      }
    }

    try {
      while (!executor.isTerminated) {}
    } catch {
      case ex: InterruptedException => {
        ex.printStackTrace()
        throw new RuntimeException(ex);
      }
    } finally {
      kafkaProducer.close()
      println("Finished Application - Closing Kafka Producer")
    }

  }
}
