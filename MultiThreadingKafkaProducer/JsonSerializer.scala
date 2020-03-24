import java.util

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.Serializer

class JsonSerializer extends Serializer[JsonNode] {

  val objectMapper: ObjectMapper = new ObjectMapper()

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def serialize(topic: String, data: JsonNode): Array[Byte] = {

    if (data.isNull) return null

    try {
      objectMapper.writeValueAsString(data).getBytes()
    } catch {
      case ex: JsonProcessingException =>
        throw new SerializationException("Error serializing JSON message" + ex)
    }
  }

  override def close(): Unit = {}

}
