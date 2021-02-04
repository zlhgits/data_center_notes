package com.zlh.cache.guava;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

/**
 * @package com.zlh.cache.guava
 * @company: dacheng
 * @author: zlh
 * @createDate: 2020/7/28
 */
@Slf4j
public class GuavaCallableCache {
    public static Cache<String, String> cache = CacheBuilder.newBuilder().maximumSize(1000).build();


    public static Object get(String key){
        String result = null;
        try {
            result = cache.get(key, new Callable<String>() {
                @Override
                public String call() {
                    System.out.println("call() key");
                    return "result:"+key;
                }
            });
        } catch (ExecutionException e) {
            log.error(":{ }",e);
        }
        return result;
    }

    public static void main(String[] args) {
        cache.put("key","value");
        System.out.println(get("key"));
        System.out.println(cache.getIfPresent("key"));
        System.out.println(get("key1"));
    }
}
