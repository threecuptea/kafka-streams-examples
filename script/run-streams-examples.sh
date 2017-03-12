
if [ -z "$SCALA_VERSION" ]; then
  SCALA_VERSION=2.12.1
fi

base_dir=$(dirname $0)

for file in "$base_dir"/streams/examples/build/libs/kafka-streams-examples*.jar;
do  
  if [ -z "$CLASSPATH" ] ; then
    CLASSPATH="$file"
  else
    CLASSPATH="$CLASSPATH":"$file"
  fi	
done

for dir in "$base_dir"/streams/examples/build/dependant-libs-${SCALA_VERSION}*;
do
  CLASSPATH="$CLASSPATH:$dir/*"
done

for file in "$base_dir"/streams/build/libs/kafka-streams*.jar;
do   
  CLASSPATH="$CLASSPATH":"$file"
done

echo "$CLASSPATH"

java -cp "$CLASSPATH" "$@"

