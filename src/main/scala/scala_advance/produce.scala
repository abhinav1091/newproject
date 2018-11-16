package scala_advance


import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit
import java.util.{Date, Properties}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}



object produce  {
  val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val simIDs = 10000 to 99999 //99000
  val brokers = "192.168.100.211:6667,192.168.100.212:6667,192.168.100.213:6667";
  val topic = "newTest";
  val props = new Properties
  props.put("bootstrap.servers", brokers)
  props.put("client.id", "Producer")
  props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  val producer = new KafkaProducer[Integer, String](props)

  while (true) {
   /* for (simID <- simIDs) {
      val data = Data(
        "64846867247",
        "?D" + simID,
        formatter.format(new Date()),
        121.503,
        31.3655,
        78,
        0,
        42,
        52806.7
      )

        println(Data.getString(data))
      producer.send(new ProducerRecord[Integer, String](topic, Data.getString(data)))
        TimeUnit.NANOSECONDS.sleep(100)

    }
    println("-------------------------------"+new Date())
    TimeUnit.MINUTES.sleep(18)
  */}
}