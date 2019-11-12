
import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * 实时热门商品 : 每隔5分钟输出最近一小时内点击量最多的前N个商品
  *
  * 业务分解：
  *     1.抽取出业务时间戳，告诉Flink框架基于业务时间做窗口
  *     2.过滤出点击行为数据
  *     3.按一小时的窗口大小，每5分钟统计一次，做滑动窗口聚合（Sliding Window）
  *     4.按每个窗口聚合，输出每个窗口中点击量前N名的商品
  */
object HotItems {
    def main(args: Array[String]): Unit = {
        
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        
        //Time类型为EventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        // 为了打印到控制台的结果不乱序，我们配置全局的并发为1，这里改变并发对结果正确性没有影响
        env.setParallelism(1)
    
        val stream = env
                // 以window下为例，需替换成自己的路径
                .readTextFile("C:\\Users\\Archer\\IdeaProjects\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
                .map(line => {
                    val linearray = line.split(",")
                    UserBehavior(linearray(0).toLong, linearray(1).toLong, linearray(2).toInt, linearray(3), linearray(4).toLong)
                })
                // 指定如何获得业务时间(通过时间戳)，以及生成Watermark
                .assignAscendingTimestamps(_.timestamp * 1000)
                //过滤出点击事件
                .filter(_.behavior == "pv")
                //设置滑动窗口
                .keyBy("itemId")
                .timeWindow(Time.minutes(60),Time.minutes(5))
                .aggregate(new CountAgg,new WindowResultFunction) //窗口聚合分为：增量窗口聚合和全窗口增量聚合，这里联合使用
                .keyBy("windowEnd")
                .process(new TopHotItems(3))
                        
        stream.print()
        
        env.execute("FlinkPro -- HotItems")
    }
    
    
    
    
}
// 求某个窗口中前 N 名的热门点击商品，key 为窗口时间戳，输出为 TopN 的结果字符串
class TopHotItems(topSize: Int) extends KeyedProcessFunction[Tuple,ItemViewCount,String]{
    
    private var itemState : ListState[ItemViewCount] = _
    
    override def open(parameters: Configuration): Unit = {
        super.open(parameters)
    
        // 命名状态变量的名字和状态变量的类型
        val itemsStateDesc = new ListStateDescriptor[ItemViewCount]("itemState-state", classOf[ItemViewCount])
    
        // 定义状态变量
        itemState = getRuntimeContext.getListState(itemsStateDesc)
    }
    override def processElement(input: ItemViewCount,
                                context: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context,
                                collector: Collector[String]): Unit = {
       //每条数据都保存到状态中
        itemState.add(input)
    
        // 注册 windowEnd+1 的 EventTime Timer, 当触发时，说明收齐了属于windowEnd窗口的所有商品数据
        // 也就是当程序看到windowend + 1的水位线watermark时，触发onTimer回调函数
        
        context.timerService().registerEventTimeTimer(input.windowEnd + 1)
    }
    
    
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
        // 获取收到的所有商品点击量
        val allItems:ListBuffer[ItemViewCount] = ListBuffer()
        import scala.collection.JavaConversions._
        for (item <- itemState.get) {
            allItems += item
        }
    
        // 提前清除状态中的数据，释放空间
        itemState.clear()
    
        // 按照点击量从大到小排序
        val sortedItems = allItems.sortBy(_.count)(Ordering.Long.reverse).take(topSize)
    
        // 将排名信息格式化成 String, 便于打印
        val result: StringBuilder = new StringBuilder
        result.append("====================================\n")
        result.append("时间: ").append(new Timestamp(timestamp - 1)).append("\n")
    
        for(i <- sortedItems.indices){
            val currentItem: ItemViewCount = sortedItems(i)
            // e.g.  No1：  商品ID=12224  浏览量=2413
            result.append("No").append(i+1).append(":")
                    .append("  商品ID=").append(currentItem.itemId)
                    .append("  浏览量=").append(currentItem.count).append("\n")
        }
        result.append("====================================\n\n")
        // 控制输出频率，模拟实时滚动结果
        Thread.sleep(1000)
        
        out.collect(result.toString())
    }
}


case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

class CountAgg extends AggregateFunction[UserBehavior,Long,Long]{
    override def createAccumulator(): Long = 0L
    
    override def add(in: UserBehavior, acc: Long): Long = acc + 1
    
    override def getResult(acc: Long): Long = acc
    
    override def merge(acc1: Long, acc2: Long): Long = acc1 + acc2
}

class WindowResultFunction extends WindowFunction[Long,ItemViewCount,Tuple,TimeWindow]{
    override def apply(key: Tuple,
                       window: TimeWindow,
                       aggregateResult: Iterable[Long],
                       out: Collector[ItemViewCount]): Unit = {
        val itemID = key.asInstanceOf[Tuple1[Long]].f0
        val count = aggregateResult.iterator.next
        
        out.collect(ItemViewCount(itemID,window.getEnd,count))
    }
}
