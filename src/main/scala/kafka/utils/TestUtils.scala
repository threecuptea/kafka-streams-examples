package kafka.utils

import kafka.server.{KafkaConfig, KafkaServer}
import org.apache.kafka.common.utils.Time

/**
  * Java class cannot call directly scala class of constructor with default parameters.  I cannot
  * call new KafkaServer(kafkaConfig, Time). Add this intermediate scala class to get it work just like Kafka source code does
  *
  *  However, both KafkaEmbedded and EmbeddedKafkaServer depend upon this.  Need some work to make sure scala class
  *  was compiled before java class
  */
object TestUtils {

  def createServer(config: KafkaConfig, time: Time = Time.SYSTEM): KafkaServer = {
    val server = new KafkaServer(config, time)
    server.startup()
    server
  }

}
