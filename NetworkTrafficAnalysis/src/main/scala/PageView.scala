import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * 网站总浏览量（PV）的统计
  * 用户每次打开一个页面便记录1次PV，多次打开同一页面则浏览量累计
  */
object PageView {
    
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        env.setParallelism(1)
    
        val stream = env.readTextFile("C:\\Users\\Archer\\IdeaProjects\\UserBehaviorAnalysis\\NetworkTrafficAnalysis\\src\\main\\resources\\UserBehavior.csv")
                .map(data => {
                    val dataArray = data.split(",")
                    UserBehavior(dataArray(0).toLong, dataArray(1).toLong, dataArray(2).toInt, dataArray(3), dataArray(4).toLong)
                })
                .assignAscendingTimestamps(_.timestamp * 1000)
                .filter(_.behavior == "pv")
                .map(x => ("pv", 1))
                .keyBy(_._1)
                .timeWindow(Time.seconds(60 * 60))
                .sum(1)
                .print()
    
        env.execute("Page View Job")
        
    }
}

case class UserBehavior(userId: Long,
                        itemId: Long,
                        categoryId: Int,
                        behavior: String,
                        timestamp: Long)