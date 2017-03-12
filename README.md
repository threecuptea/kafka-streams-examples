### spark-tutorial_2 collects small projects that I worked on using Kafka streams.  This is the first time I write java 8 lambda  and gradle.  Those projects were inspired by kafka-src streams-examples.  Plan to include scala examples too.
#### The topics include:
   1. WordCountLambdaDemo (working example)
      The instruction to run integration tests is in javadoc of that class
       
      To run the streams-examples in Kafka-src, copy run-streams-examples.sh that I put together under script folder
      to kafka-src directory, switch o that directory 
      gradle
      ./gradlew streams:examples:tasks
      ./gradlew clean -PscalaVersion=2.12 streams:examples:jar
      ./run-streams-examples.sh org.apache.kafka.streams.examples.wordcount.WordCountDemo
      
   2. PageViewRegionLambda (Working progress)   
      
      