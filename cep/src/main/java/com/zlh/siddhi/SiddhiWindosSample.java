package com.zlh.siddhi;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.stream.output.StreamCallback;
import lombok.extern.slf4j.Slf4j;


/**
 * 基本语法
 * from <input stream name>[<filter condition>]#window.<window name>(<parameter>, <parameter>, ... )
 * select <attribute name>, <attribute name>, ...
 * insert [current events | expired events | all events] into <output stream name>
 * 参考自：https://www.cnblogs.com/fxjwind/p/4991659.html
 * @package com.zlh.siddhi
 * @company: dacheng
 * @author: zlh
 * @createDate: 2020/7/22
 */
@Slf4j
public class SiddhiWindosSample {
    public static void main(String[] args) throws InterruptedException {
        //创建manager
        SiddhiManager siddhiManager = new SiddhiManager();
        //创建siddhi application
        /**
         * 另外第窗口还有
         * #window.externalTime(time, 3 sec)：按外部时间触发，根据传入的time触发3秒的过期
         */// #window.cron('*/4 * * * * ?')：书写cron表达式，每4秒触发一次滚动窗口
         /** window.unique, window.firstUnique：事件每一次都触发及只触发第一次
         * window.sort：当window length >3时，即4，会输出按price升序排序，最大的那个event
          * window.lengthBatch;timeBatch：适合用作滚动统计，即不使用events语句，直接insert into outputStream;
         */
//        String siddhiApp = getWindosLengthApp();
        String siddhiApp = getWindosTimeApp();

        //生成runTime
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        //生成input
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        //添加callback(回调函数)以从流中检索输出事件
        siddhiAppRuntime.addCallback("outputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                for (Event event:events){
                    //打印到控制台
                    System.out.println(event);
                }
            }
        });
        //启动
        siddhiAppRuntime.start();
        //发送数据
        sendWindosLengthData(inputHandler);

        //关闭runtime
        siddhiAppRuntime.shutdown();
        //关闭Siddhi Manager
        siddhiManager.shutdown();
    }

    private static void sendWindosLengthData(InputHandler inputHandler) throws InterruptedException {
        int i = 0;
        while (i < 10) {
            float p = i*10;
            inputHandler.send(new Object[]{"WSO2", p, 100});
            System.out.println("\"WSO2\", " + p);
//            inputHandler.send(new Object[] {"IBM", p, 100});
//            System.out.println("\"IBM\", " + p);
            Thread.sleep(1000);
            i++;
        }
    }

    /**
     * window.length
     * @return
     */
    private static String getWindosLengthApp() {
        //从第6条开始滑动窗口计算，窗口第一条过期，不做聚合计算
        String expiredEvents = "define stream cseEventStream (symbol string, price float, volume long);" +
                "" +
                "@info(name = 'query1') " +
                "from cseEventStream[700 > price]#window.length(6)" +
                "select symbol, price, avg(price) as ap, sum(price) as sp, count(price) as cp " +
                "group by symbol " +
                "insert expired events into outputStream;";

        //从第一条形成窗口计算，到第6条后滑动窗口，窗口第一条计算
        String currentEvents = "define stream cseEventStream (symbol string, price float, volume long);" +
                "" +
                "@info(name = 'query1') " +
                "from cseEventStream[700 > price]#window.length(6)" +
                "select symbol, price, avg(price) as ap, sum(price) as sp, count(price) as cp " +
                "group by symbol " +
                "insert current events into outputStream;";

        //expired event和current event两种情况都触发
        String allEvents = "define stream cseEventStream (symbol string, price float, volume long);" +
                "" +
                "@info(name = 'query1') " +
                "from cseEventStream[700 > price]#window.length(6)" +
                "select symbol, price, avg(price) as ap, sum(price) as sp, count(price) as cp " +
                "group by symbol " +
                "insert all events into outputStream;";
        return allEvents;
    }

    /**
     * window.time
     * @return
     */
    private static String getWindosTimeApp() {
        //从第6条开始滑动窗口计算，窗口第一条过期，不做聚合计算
        String expiredEvents = "define stream cseEventStream (symbol string, price float, volume long);" +
                "" +
                "@info(name = 'query1') " +
                "from cseEventStream[700 > price]#window.time(2 sec)" +
                "select symbol, price, avg(price) as ap, sum(price) as sp, count(price) as cp " +
                "group by symbol " +
                "insert expired events into outputStream;";

        //从第一条形成窗口计算，到第6条后滑动窗口，窗口第一条计算
        String currentEvents = "define stream cseEventStream (symbol string, price float, volume long);" +
                "" +
                "@info(name = 'query1') " +
                "from cseEventStream[700 > price]#window.time(2 sec)" +
                "select symbol, price, avg(price) as ap, sum(price) as sp, count(price) as cp " +
                "group by symbol " +
                "insert current events into outputStream;";

        //expired event和current event两种情况都触发
        String allEvents = "define stream cseEventStream (symbol string, price float, volume long);" +
                "" +
                "@info(name = 'query1') " +
                "from cseEventStream[700 > price]#window.time(2 sec)" +
                "select symbol, price, avg(price) as ap, sum(price) as sp, count(price) as cp " +
                "group by symbol " +
                "insert all events into outputStream;";
        return currentEvents;
    }
}
