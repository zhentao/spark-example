rm -rf output-java

spark-submit --class com.zhentao.spark.java.MaxTemperature --master local target/spark-example-0.0.1-SNAPSHOT.jar sample.txt output-java

rm -rf output-scala
spark-submit --class com.zhentao.spark.scala.MaxTemperature --master local target/spark-example-0.0.1-SNAPSHOT.jar sample.txt output-scala

spark-submit --class com.zhentao.spark.scala.prize.Prize --master local target/spark-example-0.0.1-SNAPSHOT.jar movie_titles.txt random-rating.txt prize-scala
