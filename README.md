### kafka-streams-example collects small projects that I worked on using Kafka streams.  This is the first time I wrote java 8 lambda and built with gradle.  Those projects were inspired by kafka-src streams-examples.  I plan to include scala examples too.
#### The topics include:
   1. WordCountLambdaDemo (working example)
      The instruction to run integration tests by starting real Kafka server etc. is in javadoc of that class
       
      To run WordCountDemo in streams-examples of Kafka-src 
      
        - copy run-streams-examples.sh that I put together under script folder to the kafka-src directory
        - change to that directory 
        - gradle
        - ./gradlew streams:examples:tasks (if you want to find out what gradle task to use)
        - ./gradlew clean -PscalaVersion=2.12 streams:examples:jar
        - ./run-streams-examples.sh org.apache.kafka.streams.examples.wordcount.WordCountDemo
            
   2. EmbeddedKafkaServer   
      I added this class to try to make WordCountLambdaDemo integration test work.  I ported this referencing multiple 
      sources and encounter multiple issues
      
      *  Java class cannot call directly scala class having a constructor with default parameters.  KafkaServer 
         has the following constructor
           _class KafkaServer(val config: KafkaConfig, time: Time = Time.SYSTEM, threadNamePrefix: 
             Option[String] = None, kafkaMetricsReporters: Seq[KafkaMetricsReporter] = List())_
         I cannot call new KafkaServer(kafkaConfig, time) or new KafkaServer(kafkaConfig) from java code.
         I have to pass in Option and Seq parameters which are not java classes as well.  
         http://lampwww.epfl.ch/~michelou/scala/using-scala-from-java.html is an excellenet article regarding to 
         how to call scala code from java code (ex. KafkaConfig$.MODULE$.BrokerIdProp()).  
         
         It turns out the best way to get around is to add an intermediate scala class to bridge them.  I added
         KafkaServerBridge scala class with createServer method     
           _def createServer(config: KafkaConfig, time: Time = Time.SYSTEM): KafkaServer = {
             val server = new KafkaServer(config, time)
             server.startup()
             server
            }_
         I still has to pass in Time but it is org.apache.kafka.common.utils.Time java class   
       * Both KafkaEmbedded and EmbeddedKafkaServer depend upon KafkaServerBridge scala class.  KafkaServerBridge 
         scala class has to be compiled before java classes. See SourceSet section in build.gradle to see how it works.
             
       *  EmbeddedKafkaServer extends from _org.junit.rules.ExternalResource_.  We can override its _before_ and _after_ 
          method to ensure that set up an external resource before a test and guarantee to tear it down afterward   
          Any intergrationTest can just add
          _@ClassRule
              public static final EmbeddedKafkaServer SERVER = new EmbeddedKafkaServer();_
          to enforec the rule. ExternalResource plus TemporaryFolder are handy tools for _junit Rule_    
          
   3.  WordCountIntegrationTest/WordCountIncludedTest
   
       I wrote WordCountIntegrationTest to start EmbeddedKafkaServer, create wordcount in/out topic, pass bootstrap
       server(host:port) of the EmbeddedKafkaServer to WordCountLambdaDemo to start KafkaStream.  Then I use
       TestUtils to publish message to wordcount in topic and to consume ConsumerRecords 
       (I ports from kafka-src IntegrationTestUtils and TestUtils) within timeout.  
       I couldn't get it to work.
        
       I wrote WordCountIncludedTest to trouble shoot.  It's easier to manipulate StreamConfig parameters in
       this way.  I manipulated cache.max.bytes.buffering and set it to 0.  I got expected output.   
       However, I won't get correct result for the default cache.max.bytes.buffering (= 10 * 1024 * 1024) until I saw 
       https://cwiki.apache.org/confluence/display/KAFKA/KIP-63%3A+Unify+store+and+downstream+caching+in+streams 
       and manipulate another StreamConfig parameter commit.interval.ms. 
       
       cache.max.bytes.buffering is a new added parameter to serves as an unified mechanism for all processing nodes.
       (Please read the above link). The semantics of this parameter is that data is forwarded and flushed whenever 
       the earliest of commit.interval.ms which specifies the frequency with which a processor flushes its state or 
       cache.max.bytes.buffering hits its limit. These are global parameters in the sense that they apply to all 
       processor nodes in the topology,
       
       Let's say the word-count input lines are
       "all streams lead to kafka"
       "hello kafka streams"
       "join kafka summit"
       
       KafkaStreams would output word count line as soon as it receive a input line and calculate counts 
       when cache.max.bytes.buffering = 0 so that the expected output would be
       - all 1
       - streams 1
       - lead 1
       - to	1
       - kafka 1
       - hello 1
       - kafka 2
       - streams 2
       - join 1
       - kafka 3
       - summit	1
       
       when cache.max.bytes.buffering is high, it would de-duplicate output lines by key and only send the lastest 
       ones.  Therefore,  we are supposed to see
       - all 1
       - lead 1
       - to	1
       - hello 1
       - streams 2
       - join 1
       - kafka	3
       - summit	1
       
       However, that data is only forwarded and flushed whenever the earliest of commit.interval.ms and 
       cache.max.bytes.buffering hits its limit (10MB here).  The default value of commit.interval.ms is 30000 and
       we set timeout to 10 sec when calling TestUtils.waitUntilMinKeyValueRecordsReceived. That's why we received 
       0 records all the time before we shorten commit.interval.ms to less than 10 second. It works like a charm 
       once we set commit.interval.ms < 10 sec.  Refer to 
       http://docs.confluent.io/current/streams/developer-guide.html#streams-developer-guide for more details 
         
       WordCountIncludedTest use org.junit.runners.Parameterized and WordCountIntegrationTest test against 
       WordCountDemo. They both are very cool Kafka streams integration tests.       
       
   4.  SimpleJoinIncludedTest
   
       A simple integration test using user-click joined with user-region then re-key into region, group, reduce
       It is inspired by kafka-src KStreamKTableJoinIntegrationTest.
       
       The take aways lesson - publish user-region (lookup table) before publish user-click otherwise 
       region would be "UNKNOWN" all the way
       
       I can assert actualOutput with expectedOutput successfully here. 
       
   5. WordCountIncludedTest enhancement and deep understanding       
       * Stream configuration key.serde and value.serde are default to ByteArraySerde if not specified. 
         You should explicitly specify them if working with text to avoid error       
       * There is an implicit form of groupByKey
           _groupByKey()_      
         Kafka would substitute it with the explicit form
           _groupByKey(final Serde<K> keySerde, final Serde<V> valSerde);_         
         using what you specify in key.serde and value.serde Streams configuration.  Throw exception if either one 
         mismatches.  Therefore, use the explicit form if key or value serde are different from the one specified 
         in Streams configuration.  For example, in testWordCountIncluded2(), I have to use 
         groupByKey(Serdes.String(), Serdes.Long()) since the default value serde is defined as Serdes.String().       
       * Yes, there are two ways to generate word count after flatMap lines into individual words.  
           _.groupBy((key, value) -> value)
            .count("Counts");_
            
           _.map((key, value) -> new KeyValue<>(value, 1L))
            .groupByKey(Serdes.String(), Serdes.Long()) 
            .reduce((v1, v2) -> v1 + v2, "Counts");_    
            
   6.  StockTransactionWindowDemo is inspired by https://github.com/bbejeck/kafka-streams.  I rewrote it to output 
       StockWindow instead of Windowd<String> directly because kafka-console-consumer.sh cannot display Windowed 
       timestamp
       * It suffers the same issue as https://stackoverflow.com/questions/44049877.  Kafka Stream commit makes 
         particular window to get pushed multiple times to a topic as commit is triggered during that window.
         The problem is that Kafka memory internals: commit.interval.ms and cache.max.bytes.buffering continue to play 
         a role when Kafka-stream output its results.  TimeWindow boundary is based upon epoch time(ex. 
         May 28, 2017 1:15:20 PM, May 28, 2017 1:15:30 PM for 10 sec.)  commit.interval.ms might be based upon 
         application start-up time.  They cannot be in sync with small TimeWindow and not to say big TimeWindow like
         1 day or 1 week.  What value of cache.max.bytes.buffering should we set for in this case?
               
       * In my opinion, Kafka-streams should honor TimeWindow expectation regardless internal memory management.  Kept
         commit results in the state store and not to output.  When TimeWindow hit timeout limit, it should add 
         uncommited summary to the last committed one then output.          

       * I like POLO JSON Serdes using gson implementation (JsonSerializer & JsonDeserializer in 
         org.freemind.kafka.streams.examples.serializer package.
                   
           _JsonSerializer<StockTransaction> trxSerializer = new JsonSerializer<>();
            JsonDeserializer<StockTransaction> trxDeserializer = new JsonDeserializer<>(StockTransaction.class);
            Serde<StockTransaction> trxSerde = Serdes.serdeFrom(trxSerializer, trxDeserializer);_ 
            
         The above is an example of Serde<StockTransaction>.  It only takes 3 lines of codes.  This is much better than 
         PageViewTypedDemo in kafka-src. That is using fastxml jackson implementation and it takes 7 lines for each 
         POLO JSON Serdes.  That's why I am using untype one in PageViewRegionLambda. However, Untype JSON Serdes 
         implemented using JsonNode and JsonNodeFactory are just not as natural as POLO JSON Serdes                
         
   I will study Spark structured streaming solution (Spark calls it SlidingWindow) first.  Then come back to finish
   KStreamKStream, KTableKTable, KStreamGlobalTable, KStreamAggregation include/integration tests. 
   http://docs.confluent.io/current/streams/developer-guide.html is excellent docuement.