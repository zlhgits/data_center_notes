package com.zlh.siddhi;

import com.alibaba.fastjson.JSONObject;
import com.zlh.util.KafkaSinkUtil;
import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.query.output.callback.QueryCallback;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.stream.output.StreamCallback;
import io.siddhi.core.util.EventPrinter;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ExecutionException;

/**
 * Siddhi是一个轻量级的，简单的开源的复杂事件流程引擎。它使用类SQL的语言描述事件流任务，可以很好的支撑开发一个可扩展的，可配置的流式任务执行引擎
 * 官网例子
 * 该示例演示了如何在另一个Java程序中使用Siddhi。
 *  此示例包含一个简单的过滤器查询。
 * @package com.zlh
 * @company: dacheng
 * @author: zlh
 * @createDate: 2020/7/21
 */
@Slf4j
public class SimpleFilterSample {
    public static void main(String[] args) throws InterruptedException {
        new SimpleFilterSample().test2();
    }

    private void test2() throws InterruptedException {
        //创建Siddhi Manager
        SiddhiManager siddhiManager = new SiddhiManager();
        String id = "2000";
        //创建Siddhi应用 DSL
        String siddhiApp = "@App:name('simpleTest')" +
                "define stream StockStream (symbol string, price float, volume long);" +
                "define stream OutputStream(symbol string, value string,id double); " +
//                "@info(name = 'query1') " +
//                "from StockStream " +
//                "select symbol, price as value,120 as id " +
//                "insert into OutputStream;"+
                "@info(name = 'query2') " +
                "from StockStream " +
                "select convert(symbol,'string') as symbol, convert(volume,'string') as value ,convert("+id+",'double') as id " +
                "insert into OutputStream;";
        //生成runtime
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        //获取InputHandler以将事件推送到Siddhi
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("StockStream");
        //添加callback(回调函数)以从流中检索输出事件
        siddhiAppRuntime.addCallback("OutputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                //发送到socket
                EventPrinter.print(events);
            }
        });
        //开始处理
        siddhiAppRuntime.start();
        System.out.println("*************************: "+siddhiAppRuntime.getName());
        //发送事件到Siddhi
        inputHandler.send(new Object[]{"IBM", 700f, 100L});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 200L});
        inputHandler.send(new Object[]{"GOOG", 50f, 30L});
        inputHandler.send(new Object[]{"IBM", 76.6f, 400L});
        inputHandler.send(new Object[]{"WSO2", 45.6f, 50L});
        Thread.sleep(500);

        inputHandler.send(new Object[]{"ZHANG2", 45.6f, 51L});
        inputHandler.send(new Object[]{"WANG2", 45.6f, 52L});

        siddhiAppRuntime.shutdown();
    }
    private void test1() throws InterruptedException {
        //创建Siddhi Manager
        SiddhiManager siddhiManager = new SiddhiManager();
        //创建Siddhi应用 DSL
        String siddhiApp = "@App:name('simpleTest')" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@info(name = 'query1') " +
                "from StockStream[volume < 150] " +
                "select symbol, price " +
                "insert into OutputStream;";
        String siddhiApp2 = "@App:name('simpleTest')" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@info(name = 'query1') " +
                "from StockStream[volume < 150] " +
                "select symbol, volume " +
                "insert into OutputStream;";
        //生成runtime
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        //获取InputHandler以将事件推送到Siddhi
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("StockStream");
        //添加callback(回调函数)以从流中检索输出事件
        QueryCallback queryCallback = new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
            }
        };
//        siddhiAppRuntime.addCallback("OutputStream", new StreamCallback() {
//            @Override
//            public void receive(Event[] events) {
//                //发送到socket
//                EventPrinter.print(events);
//                for (Event event:events){
//                    //打印到控制台
//                    System.out.println(event.getData(0)+" : "+event.getData(1));
//                    //发送到kafka
////                    try {
////                        KafkaSinkUtil.send(JSONObject.toJSONString(toMap(event)));
////                    } catch (ExecutionException e) {
////                        log.debug(":{}",e);
////                    } catch (InterruptedException e) {
////                        log.debug(":{}",e);
////                    }
//                }
//            }
//        });

        siddhiAppRuntime.addCallback("query1",queryCallback);

        //开始处理
        siddhiAppRuntime.start();
        System.out.println("*************************: "+siddhiAppRuntime.getName());
        //发送事件到Siddhi
        inputHandler.send(new Object[]{"IBM", 700f, 100L});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 200L});
        inputHandler.send(new Object[]{"GOOG", 50f, 30L});
        inputHandler.send(new Object[]{"IBM", 76.6f, 400L});
        inputHandler.send(new Object[]{"WSO2", 45.6f, 50L});
        Thread.sleep(500);
        SiddhiAppRuntime simpleTest1 = siddhiManager.getSiddhiAppRuntime("simpleTest");
        //关闭runtime
//        siddhiAppRuntime.shutdown();

//        simpleTest1.shutdown();

        siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp2);
        inputHandler = siddhiAppRuntime.getInputHandler("StockStream");
        siddhiAppRuntime.addCallback("query1",queryCallback);
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"ZHANG2", 45.6f, 51L});
        inputHandler.send(new Object[]{"WANG2", 45.6f, 52L});

        siddhiAppRuntime.shutdown();
        //关闭Siddhi Manager
//        siddhiManager.shutdown();
    }
}
