
import OrderTimeout.{OrderEvent, OrderResult}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
/**
  * 可以在订单的create事件到来后注册定时器，15分钟后触发；
  * 然后再用一个布尔类型的Value状态来作为标识位，表明pay事件是否发生过。
  * 如果pay事件已经发生，状态被置为true，那么就不再需要做什么操作；
  * 而如果pay事件一直没来，状态一直为false，到定时器触发时，就应该输出超时报警信息。
  */
object OrderTimeoutWithoutCep {
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
         .keyBy(_.orderId)
    
        // 自定义一个 process function，进行order的超时检测，输出超时报警信息
        val timeoutWarningStream  = orderEventStream.process(new OrderTimeOutAlert)
        
        timeoutWarningStream.print()
        env.execute()
    }
    
    class OrderTimeOutAlert extends KeyedProcessFunction[Long,OrderEvent,OrderResult]{
       
        lazy val isPayStatus:ValueState[Boolean] = getRuntimeContext.getState(
            new ValueStateDescriptor[Boolean]("ispayed-state",classOf[Boolean])
        )
        
        override def processElement(ele: OrderEvent,
                                    context: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context,
                                    out: Collector[OrderResult]): Unit = {
            val isPayed = isPayStatus.value()
            
            if (ele.eventType == "create" && !isPayed) {
                context.timerService().registerEventTimeTimer(ele.eventTime * 1000L)
            } else if (ele.eventType == "pay") {
                isPayStatus.update(true)
            }
            
        }
    
    
        override def onTimer(timestamp: Long,
                             ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext,
                             out: Collector[OrderResult]): Unit = {
            val isPayed = isPayStatus.value()
            
            if (! isPayed) {
                out.collect(OrderResult(ctx.getCurrentKey,"order timeout"))
            }
            
            isPayStatus.clear()
        }
    }
    
}
