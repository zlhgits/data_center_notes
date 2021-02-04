package com.zlh.siddhi;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.stream.output.StreamCallback;
import lombok.extern.slf4j.Slf4j;

/**
 * @package com.zlh.siddhi
 * @company: dacheng
 * @author: zlh
 * @createDate: 2020/7/28
 */
@Slf4j
public class KafkaSiddhiSample {
    private static void kafkaSource(){
        SiddhiManager siddhiManager = null;
        SiddhiAppRuntime siddhiAppRuntime = null;
        try{
            String app = "define stream BarStream (symbol string, price string, volume1 string); \n" +
                    "@info(name = 'query1') \n" +
                    "@source(\n" +
                    "type='kafka', \n" +
                    "topic.list='dc_waf', \n" +
                    "group.id='test26', \n" +
                    "threading.option='partition.wise', \n" +
                    "bootstrap.servers='192.168.50.98:9092', \n" +
                    "partition.no.list='0,1,2', \n" +
                    "@map(type='json'))\n" +
                    "Define stream FooStream (" +
                    "symbol string, price string, volume string,volume1 string" +
                    ");\n" +
                    "from FooStream select symbol, price, volume as volume1 insert into BarStream;";

            String app_2 = "define stream BarStream (devType string,KTitle string,srcIP string); \n" +
                    "@info(name = 'query1') \n" +
                    "@source(\n" +
                    "type='kafka', \n" +
                    "topic.list='tz.collide.data', \n" +
                    "group.id='test8', \n" +
                    "threading.option='partition.wise', \n" +
                    "bootstrap.servers='192.168.50.98:9092', \n" +
                    "partition.no.list='0,1,2', \n" +
                    "@map(type='json'))\n" +
                    "Define stream FooStream (" +
                    "devType string,dstGeoPoint string,KTitle string,srcIP string,dstProvider string,attackerIpType string,threatSubType string,srcPort string,victimPort string,victimCity string,attackDirection string,\n" +
                    "victimIP string,srcProvider string,victimProvider string,devSubType string,attackerProvider string,dstIP string,id string,threatCode string,incidentLevel string,dataType string,devVersion string,attackerPort string,\n" +
                    "KSolution string,threatType string,attackerGeoPoint string,subType string,victimGeoPoint string,ruleID string,attackStage string,KDesc string,threatSubTypeEn string,status string,dstIpType string,logType string,dstProvince string,\n" +
                    "attackerIP string,appProtocol string,threatTypeEn string,srcCountry string,stamp string,httpMethod string,URL string,srcProvince string,dstPort string,attackerProvince string,victimCountry string,victimProvince string,httpAgent string,\n" +
                    "transportProtocol string,dstCountry string,eventTime string,collideTime string,incidentName string,KRef string,attackerCountry string,srcIpType string,srcGeoPoint string,victimIpType string,dstHostName string,dstCity string" +
                    ");\n" +
                    "from FooStream select devType, KTitle, srcIP insert into BarStream;";

            String app_3 = "define stream BarStream (base string,attackerCountry string); \n" +
                    "@source(\n" +
                    "type='kafka', \n" +
                    "topic.list='tz.standard.data', \n" +
                    "group.id='test2', \n" +
                    "threading.option='partition.wise', \n" +
                    "bootstrap.servers='192.168.50.98:9092', \n" +
                    "partition.no.list='0,1,2', \n" +
                    "@map(type='json',@attributes(base = '$')))\n" +
                    "Define stream FooStream (" +
                    "base string" +
                    ");\n" +
                    "@info(name = 'query1') \n" +
                    //不能使用别名做条件
                    "from FooStream[json:getString(base,'$.devType') == 'ips' and json:getString(base,'$.dstCountry') == '中国']#window.length(1)" +
                    " select json:getString(base,'$.devType') as base,json:getString(base,'$.dstCountry') as attackerCountry insert into BarStream;";
            //获取manager
            siddhiManager = new SiddhiManager();
            siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(app_3);
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
        }catch (Exception e){
            log.error("{ }:",e);
            siddhiAppRuntime.shutdown();
            siddhiManager.shutdown();
        }
    }

    private static void kafkaSink(){
        SiddhiManager siddhiManager = null;
        SiddhiAppRuntime siddhiAppRuntime = null;
        try{
            String app = "@App:name('kafkaSinkTest') \n" +
                    "define stream FooStream (symbol string, price float, volume long); \n" +
                    "@info(name = 'query1') \n" +
                    "@sink(\n" +
                    "type='kafka',\n" +
                    "topic='dc_waf',\n" +
                    "partition.no='0',\n" +
                    "bootstrap.servers='192.168.50.98:9092',\n" +
                    "@map(type='json'))\n" +
                    "Define stream BarStream (symbol string, price float, volume long);\n" +
                    "from FooStream select symbol, price, volume insert into BarStream;";

            //获取manager
            siddhiManager = new SiddhiManager();
            siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(app);

            //InputHandle
            InputHandler inputHandler = siddhiAppRuntime.getInputHandler("FooStream");

            siddhiAppRuntime.start();

            //发送事件到Siddhi
            inputHandler.send(new Object[]{"IBM", 700f, 100L});
            inputHandler.send(new Object[]{"WSO2", 60.5f, 200L});
            inputHandler.send(new Object[]{"GOOG", 50f, 30L});
            inputHandler.send(new Object[]{"IBM", 76.6f, 400L});
            inputHandler.send(new Object[]{"WSO2", 45.6f, 50L});
            Thread.sleep(500);

            //关闭runtime
            siddhiAppRuntime.shutdown();
            //关闭Siddhi Manager
            siddhiManager.shutdown();
        }catch (Exception e){
            log.error("{ }:",e);
            siddhiAppRuntime.shutdown();
            siddhiManager.shutdown();
        }
    }
    public static void main(String[] args) {
        kafkaSource();
//        kafkaSink();
    }
}
