
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * 实时对账系统：来自两条流的订单交易匹配
  *
  * 利用connect将两条流进行连接，然后用自定义的CoProcessFunction进行处理
  */
object TxMatch {
    
    val unmatchedPays = new OutputTag[OrderEvent]("unmatchedPays")
    val unmatchedReceipts = new OutputTag[ReceiptEvent]("unmatchedReceipts")
    
    def main(args: Array[String]): Unit = {
    
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    
        val orderEventStream = env.fromCollection(
            List(OrderEvent(1001,"pay","1",20191112), OrderEvent(1002,"pay","2",20191112))
        )/*readTextFile("YOUR_PATH\\resources\\OrderLog.csv")
                .map( data => {
                    val dataArray = data.split(",")
                    OrderEvent(dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong)
                })*/
                .filter(_.txId != "")
                .assignAscendingTimestamps(_.eventTime * 1000L)
                .keyBy(_.txId)
    
    
        val receiptEventStream = env.fromCollection(
            List(ReceiptEvent("2","aliPay",20191112))
        )/*readTextFile("YOUR_PATH\\resources\\ReceiptLog.csv")
                .map( data => {
                    val dataArray = data.split(",")
                    ReceiptEvent(dataArray(0), dataArray(1), dataArray(2).toLong)
                })*/
                .assignAscendingTimestamps(_.eventTime * 1000L)
                .keyBy(_.txId)
        
        val processedStream = orderEventStream.connect(receiptEventStream).process(new TxMatchDetection)
        
        processedStream.getSideOutput(unmatchedPays).print("unmatched pays")
        processedStream.getSideOutput(unmatchedReceipts).print("unmatched receipts")
        
        processedStream.print("processed")
        env.execute()
        
    }
    
    case class OrderEvent( orderId: Long, eventType: String, txId: String, eventTime: Long )
    
    case class ReceiptEvent( txId: String, payChannel: String, eventTime: Long )
    
    
    class TxMatchDetection extends CoProcessFunction[OrderEvent,ReceiptEvent,(OrderEvent,ReceiptEvent)] {
       
       lazy val payState: ValueState[OrderEvent] = getRuntimeContext.getState(
           new ValueStateDescriptor[OrderEvent]("pay-state",classOf[OrderEvent])
       )
        
        lazy val receiptState:ValueState[ReceiptEvent] = getRuntimeContext.getState(
            new ValueStateDescriptor[ReceiptEvent]("receipt-state",classOf[ReceiptEvent])
        )
       
        override def processElement1(pay: OrderEvent,
                                     context: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context,
                                     out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
            
            val receipt = receiptState.value()
            
            if (receipt != null) {
                receiptState.clear()
                out.collect((pay,receipt))
            } else {
                payState.update(pay)
                context.timerService().registerEventTimeTimer(pay.eventTime * 1000L + 5000L)
            }
        }
    
        override def processElement2(receipt: ReceiptEvent,
                                     context: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context,
                                     out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
            val payment = payState.value()
            
            if (payment != null) {
                payState.clear()
                out.collect((payment,receipt))
            } else{
                receiptState.update(receipt)
                context.timerService().registerEventTimeTimer(receipt.eventTime * 1000L)
            }
        }
    
    
        override def onTimer(timestamp: Long,
                             ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#OnTimerContext,
                             out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
           if (payState.value() != null) {
               ctx.output(unmatchedPays,payState.value())
           }
            
            if (receiptState.value() != null) {
                ctx.output(unmatchedReceipts,receiptState.value())
            }
            
            payState.clear()
            receiptState.clear()
        }
    }
}




