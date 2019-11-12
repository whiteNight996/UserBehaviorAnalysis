import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

/**
  * 网站独立访客数（UV）的统计
  * 一段时间（比如一小时）内访问网站的总人数，1天内同一访客的多次访问只记录为一个访客。
  *
  * 1)统计埋点日志中的 pv 行为，利用 Set 数据结构进行去重
  * 2)对于超大规模的数据，可以考虑用布隆过滤器进行去重(Bloom Filter)
  *
  */
object UniqueVisitor {
    def main(args: Array[String]): Unit = {
    
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        env.setParallelism(1)
    
        val stream = env.readTextFile("C:\\Users\\Archer\\IdeaProjects\\UserBehaviorAnalysis\\NetworkTrafficAnalysis\\src\\main\\resources\\UserBehavior.csv")
                .map(line => {
                    val linearray = line.split(",")
                    UserBehavior(
                        linearray(0).toLong,
                        linearray(1).toLong,
                        linearray(2).toInt,
                        linearray(3),
                        linearray(4).toLong
                    )
                })
                .assignAscendingTimestamps(_.timestamp * 1000)
                .filter(_.behavior == "pv")
                .map(data => ("dummyKey", data.userId))
                .keyBy(_._1)
                .timeWindow(Time.seconds(15))
                .trigger(new MyTrigger)//自定义窗口触发规则
                .process(new UvCountWithBloom) //自定义窗口处理规则
        
        stream.print()
        env.execute("Unique Visitor with bloom Job")
        
    }
}

//自定义触发器
class MyTrigger extends Trigger[(String,Long),TimeWindow]{
    override def onElement(t: (String, Long), l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
        // 每来一条数据，就触发窗口操作并清空
        TriggerResult.FIRE_AND_PURGE
    }
    
    override def onProcessingTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
        TriggerResult.CONTINUE
    }
    
    override def onEventTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
        TriggerResult.CONTINUE
    }
    
    override def clear(w: TimeWindow, triggerContext: Trigger.TriggerContext): Unit = {
    
    }
}

//自定义窗口处理函数
class UvCountWithBloom extends ProcessWindowFunction[(String,Long),UvCount,String,TimeWindow]{
    
    // 创建redis连接
    lazy val jedis = new Jedis("hadoop102",6379)
    lazy val bloom = new Bloom(1 << 29)
    
    override def process(key: String,
                         context: Context,
                         elements: Iterable[(String, Long)],
                         out: Collector[UvCount]): Unit = {
        
        val storeKey= context.window.getEnd.toString
        var count = 0L
        
        if (jedis.hget("count",storeKey) != null) {
            count = jedis.hget("count",storeKey).toLong
        }
        
        val userId = elements.last._2.toString
        val offset = bloom.hash(userId,61)
        
        val isExist = jedis.getbit(storeKey,offset)
        if (! isExist) {
            jedis.setbit(storeKey,offset,true)
            jedis.hset("count",storeKey,(count + 1).toString)
            out.collect(UvCount(storeKey.toLong,count + 1))
            
        } else {
            out.collect(UvCount(storeKey.toLong,count + 1))
        }
    }
}

//定义布隆过滤器
class Bloom(size: Long) extends Serializable {
    private val cap = size
    
    def hash(value: String, seed: Int): Long = {
        var result = 0
        for (i <- 0 until value.length) {
            // 最简单的hash算法，每一位字符的ascii码值，乘以seed之后，做叠加
            result = result * seed + value.charAt(i)
        }
        (cap - 1) & result
    }
}

case class UvCount(windowEnd: Long, count: Long)

