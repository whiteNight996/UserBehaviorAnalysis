import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import scala.collection.Map

/**
  * 订单支付实时监控
  */
object OrderTimeout {
    
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        
        val orderEventStream = env.fromCollection(
            List(
                OrderEvent(1,"create", 1558430842),
                OrderEvent(2, "create", 1558430843),
                OrderEvent(2, "pay", 1558430844)
            )
        ).assignAscendingTimestamps(_.eventTime * 1000)
        
        //定义一个带匹配时间窗口的模式
        val orderPayPattern = Pattern.begin[OrderEvent]("begin")
                        .where(_.eventType == "create")
                        .followedBy("follow")
                        .where(_.eventType == "pay")
                        .within(Time.seconds(5)) //实际应该设置15分钟
        
        //定义一个输出标签
        val orderTimeoutOutput = OutputTag[OrderResult]("orderTimeout")
        
        //订单事件按照orderId进行分流，然后在每一条中匹配出定义好的模式
        val patternStream = CEP.pattern(orderEventStream.keyBy("orderId"),orderPayPattern)
    
        
        val completedResult  = patternStream.select(orderTimeoutOutput) {
            (pattern:Map[String,Iterable[OrderEvent]], timestamp :Long) => {
                val createOrder = pattern.get("begin")
                OrderResult(createOrder.get.iterator.next().orderId,"timeout")
            }
        } {
            pattern: Map[String, Iterable[OrderEvent]] => {
                val payOrder = pattern.get("follow")
                OrderResult(payOrder.get.iterator.next().orderId, "success")
            }
        }
        
        //拿到同一输出标签中的timeout匹配结果(流)
        val timeoutResult = completedResult.getSideOutput(orderTimeoutOutput)
        
        completedResult.print()
        timeoutResult.print()
        
        env.execute()
    }
    
    
    case class OrderEvent(orderId: Long, eventType: String, eventTime: Long)
    case class OrderResult(orderId: Long, eventType: String)
}
