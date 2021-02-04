package com.zlh.siddhi;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.stream.output.StreamCallback;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

/**
 * @package com.zlh.siddhi
 * @company: dacheng
 * @author: zlh
 * @createDate: 2020/8/4
 */
@Slf4j
public class HttpSinkSample {
    public static final String TASK_SQL = "@App:name('kafkaTest')" +
            "@info(name = 'queryOut') \n" +
            "@sink(\n" +
            "type='kafka',\n" +
            "topic='dc_waf',\n" +
            "partition.no='0',\n" +
            "bootstrap.servers='192.168.50.98:9092',\n" +
            "@map(type='json'))\n" +
            "define stream BarStream (devType string,KTitle string,srcIP string); \n" +

            "@info(name = 'queryInput') \n" +
            "@source(\n" +
            "type='kafka', \n" +
            "topic.list='tz.collide.data', \n" +
            "group.id='test9', \n" +
            "threading.option='partition.wise', \n" +
            "bootstrap.servers='192.168.50.98:9092', \n" +
            "partition.no.list='0,1,2', \n" +
            "@map(type='json'))\n" +
            "Define stream FooStream (" +
            "devType string,dstGeoPoint string,KTitle string,srcIP string,dstProvider string" +
            ");\n" +

            "from FooStream select devType, KTitle, srcIP insert into BarStream;";
    private static void httpSink(){
        SiddhiManager siddhiManager = null;
        SiddhiAppRuntime siddhiAppRuntime = null;
        try{
            //blocking.io='true',
            String sink = "@sink(type='http-call', method='POST',blocking.io='true', \n" +
                    "      publisher.url='http://localhost:8803/dataset/cepTest',\n" +
                    "      sink.id='employee-info', @map(type='json')) \n" +
                    "define stream EmployeeRequestStream (name string, id int);";

            String source = "@source(type='http-call-response', sink.id='employee-info',\n" +
                    "        http.status.code='2\\d+',\n" +
                    "        @map(type='json',\n" +
                    "             @attributes(name='trp:name', id='trp:id',\n" +
                    "                         location='$.town', age='$.age')))\n" +
                    "define stream EmployeeResponseStream(name string, id int,\n" +
                    "                                     location string, age int);";

            String app = "@App:name('httpSinkTest')" +
                    sink  + source +
                    "from EmployeeResponseStream " +
                    "select id, name, location,age insert into BarStream;";

            //获取manager
            siddhiManager = new SiddhiManager();
            siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(app);
            InputHandler inputHandler = siddhiAppRuntime.getInputHandler("EmployeeRequestStream");

            siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
                @Override
                public void receive(Event[] events) {
                    System.out.println("回调函数 运行");
                    for (Event event:events){
                        System.out.println(event);
                    }
                }
            });
            siddhiAppRuntime.start();

            //发送事件到Siddhi
            inputHandler.send(new Object[]{"IBM", 1});
            inputHandler.send(new Object[]{"WSO2", 2});
            inputHandler.send(new Object[]{"GOOG", 3});
            inputHandler.send(new Object[]{"IBM", 4});
            inputHandler.send(new Object[]{"WSO2", 5});
            Thread.sleep(500);

        }catch (Exception e){
            log.error("{ }:",e);
            siddhiAppRuntime.shutdown();
            siddhiManager.shutdown();
        }
    }

    public static void main(String[] args) {
        HttpSinkSample.httpSink();
    }
}
