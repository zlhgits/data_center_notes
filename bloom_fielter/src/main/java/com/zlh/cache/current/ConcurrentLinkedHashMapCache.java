package com.zlh.cache.current;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.googlecode.concurrentlinkedhashmap.EvictionListener;
import com.googlecode.concurrentlinkedhashmap.Weighers;
import lombok.extern.slf4j.Slf4j;

/**
 * ConcurrentLinkedHashMap 是google团队提供的一个容器。它有什么用呢？
 * 其实它本身是对ConcurrentHashMap的封装，可以用来实现一个基于LRU策略的缓存。
 *
 * LRU（Least recently used，最近最少使用）算法根据数据的历史访问记录来进行淘汰数据，淘汰掉最不经常使用的数据
 * @package com.zlh.cache.current
 * @company: dacheng
 * @author: zlh
 * @createDate: 2020/7/27
 */
@Slf4j
public class ConcurrentLinkedHashMapCache {
    public static void main(String[] args) {
        //该接口可以监控淘汰的数据
        EvictionListener<String, String> listener = new EvictionListener<String, String>() {
            @Override
            public void onEviction(String key, String value) {
                System.out.println("key: "+key+" ,value: "+value +",数据已淘汰");
            }
        };
        ConcurrentLinkedHashMap.Builder<String, String> builder = new ConcurrentLinkedHashMap.Builder<>();
        //定义最大容量为 3,只能存储3个
        builder.maximumWeightedCapacity(3);
        //监控淘汰数据
        builder.listener(listener);
        builder.weigher(Weighers.singleton());

        ConcurrentLinkedHashMap<String, String> linkedHashMap = builder.build();

        linkedHashMap.put("1", "1");
        linkedHashMap.put("2", "2");
        linkedHashMap.put("3", "3");

        linkedHashMap.get("1");

        linkedHashMap.put("4", "4");
        linkedHashMap.put("2", "2");
        linkedHashMap.put("5", "5");

        /**
         * 因为插入4之前get了1，所以结果为
         * 1 : 1
         * 3 : 3
         * 4 : 4
         */
        linkedHashMap.forEach((k,v)-> System.out.println(k+" : "+v));
    }
}
