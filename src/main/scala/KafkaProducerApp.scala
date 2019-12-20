import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io.Source

object KafkaProducerApp extends App {

  val url = getClass.getResource("config.properties")
  val properties: Properties = new Properties()
  properties.load(Source.fromURL(url).bufferedReader())

  val props: Properties = new Properties()
  props.put("bootstrap.servers", properties.getProperty("broker_host_port"))
  props.put("key.serializer",
    "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer",
    "org.apache.kafka.common.serialization.StringSerializer")
  props.put("acks", "all")
  props.put("max.in.flight.requests.per.connection", "10")
  val producer = new KafkaProducer[String, String](props)
  val topic = "RZA"

  var recordsNum = new String()
  if (args.isEmpty)
    recordsNum = properties.getProperty("records_count")
  else
    recordsNum = args(0)

  try {
    for (i <- 0 to Integer.valueOf(recordsNum)) {
      val record = new ProducerRecord[String, String](topic, i.toString, BookingEventGenerator.generateBookingEvent(i))
      val metadata = producer.send(record)
      printf(s"sent record(key=%s value=%s) " +
        "meta(partition=%d, offset=%d)\n",
        record.key(), record.value(),
        metadata.get().partition(),
        metadata.get().offset())
    }
  } catch {
    case e: Exception => e.printStackTrace()
  } finally {
    producer.close()
  }
}
