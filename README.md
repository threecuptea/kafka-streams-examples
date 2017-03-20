### kafka-streams-example collects small projects that I worked on using Kafka streams.  This is the first time I wrote java 8 lambda and built with gradle.  Those projects were inspired by kafka-src streams-examples.  I plan to include scala examples too.
#### The topics include:
   1. WordCountLambdaDemo (working example)
      The instruction to run integration tests is in javadoc of that class
       
      To run WordCountDemo in streams-examples of Kafka-src 
      
        - copy run-streams-examples.sh that I put together under script folder to the kafka-src directory
        - change to that directory 
        - gradle
        - ./gradlew streams:examples:tasks (if you want to find out what gradle task to use)
        - ./gradlew clean -PscalaVersion=2.12 streams:examples:jar
        - ./run-streams-examples.sh org.apache.kafka.streams.examples.wordcount.WordCountDemo
      
   2. PageViewRegionLambda (Working progress)
      
   3. EmbeddedKafkaServer   
      I added this class to try to make WordCountLambdaDemo integration test work.  I ported this referencing multiple 
      sources and encounter multiple issues
      
      *  Java class cannot call directly scala class having a constructor with default parameters.  KafkaServer 
         has the following constructor
           _class KafkaServer(val config: KafkaConfig, time: Time = Time.SYSTEM, threadNamePrefix: Option[String] = None, 
             kafkaMetricsReporters: Seq[KafkaMetricsReporter] = List())_
         I cannot call new KafkaServer(kafkaConfig, time) or new KafkaServer(kafkaConfig) from java code.  
         http://lampwww.epfl.ch/~michelou/scala/using-scala-from-java.html is an excellenet article regarding to 
         how to call scala code from java code (ex. KafkaConfig$.MODULE$.BrokerIdProp()).  
         
         It turns out the best way to get around is to add an intermediate scala class to bridge them.  I added TestUtils
         scala class with createServer method     
           _def createServer(config: KafkaConfig, time: Time = Time.SYSTEM): KafkaServer = {
             val server = new KafkaServer(config, time)
             server.startup()
             server
            }_
            
       * Both KafkaEmbedded and EmbeddedKafkaServer depend upon TestUtils scala class.  TestUtils scala class has
         to be compiled before java classes. See SourceSet section in build.gradle to see how it works.
             
       *  EmbeddedKafkaServer extends from _org.junit.rules.ExternalResource_.  We can override its _before_ and _after_ 
          method to ensure that set up an external resource before a test and guarantee to tear it down afterward   
          Any intergrationTest can just add
          _@ClassRule
              public static final EmbeddedKafkaServer SERVER = new EmbeddedKafkaServer();_
          to enforec the rule. ExternalResource plus TemporaryFolder are handy tools for _junit Rule_    
          