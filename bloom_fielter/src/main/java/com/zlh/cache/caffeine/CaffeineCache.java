package com.zlh.cache.caffeine;

import com.github.benmanes.caffeine.cache.*;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * @package com.zlh.cache.caffeine
 * @company: dacheng
 * @author: zlh
 * @createDate: 2020/7/28
 */
@Slf4j
public class CaffeineCache {
    /**
     * 手动加载
     * @throws InterruptedException
     */
    private static void handPutCase() throws InterruptedException {
        Caffeine<Object, Object> builder = Caffeine.newBuilder();
        //被访问或添加后多久过期
        builder.expireAfterAccess(5, TimeUnit.SECONDS);
        //基于缓存大小过期
        builder.maximumSize(3);
        /**
         * 手动加载市Cache
         */
        Cache<Object, Object> cache = builder.build();

        //put
        cache.put(1,"value1-1");
        System.out.println(cache.getIfPresent(1));

        //覆盖
        cache.put(1,"value1-2");
        System.out.println("覆盖 " + cache.getIfPresent(1));

        //不存在的值
        System.out.println("不存在的值 "+cache.get(2,x -> x + ":value2-1"));
        System.out.println(cache.getIfPresent(2));

        //手动过期
        cache.invalidate(2);
        System.out.println("手动淘汰 "+cache.getIfPresent(2));

        //时间过期
        TimeUnit.SECONDS.sleep(5);
        System.out.println("时间过期"+cache.getIfPresent(1));
    }

    /**
     * 同步加载
     * @throws InterruptedException
     */
    private static void syncPutCase() throws InterruptedException {
        /**
         * 同步加载是LoadingCache
         */
        LoadingCache<Integer, String> cache = Caffeine.newBuilder()
                .expireAfterAccess(10, TimeUnit.SECONDS)
                .maximumSize(5).build(new CacheLoader<Integer, String>() {
                    @Nullable
                    @Override
                    public String load(@NonNull Integer key) throws Exception {
                        if (key == 10){
                            return null;
                        }
                        return key + ":value";
                    }
                });

        System.out.println(cache.get(1));

        List list = new ArrayList(3);
        list.add(1);
        list.add(2);
        list.add(3);
        Map<Integer,String> cacheAll = cache.getAll(list);
        System.out.println(cacheAll.get(1)+"-"+cacheAll.get(2)+"-"+cacheAll.get(3));

        System.out.println(cache.getIfPresent(3));
        System.out.println(cache.getIfPresent(4));

        cache.put(4,"value4-1");
        System.out.println(cache.getIfPresent(4));
        System.out.println("缓存大小："+cache.estimatedSize());
        //返回null，数据不会缓存
        System.out.println(cache.get(10));
        System.out.println("缓存大小："+cache.estimatedSize());
        System.out.println(cache.get(10));
        System.out.println("缓存大小："+cache.estimatedSize());
        System.out.println(cache.get(12));
        System.out.println("缓存大小："+cache.estimatedSize());
    }

    /**
     * 异步加载
     */
    private static void asyncPutCase() throws ExecutionException, InterruptedException {
        AsyncLoadingCache cache = Caffeine.newBuilder()
                .expireAfterAccess(5,TimeUnit.SECONDS)
                .maximumSize(5)
                .buildAsync(new CacheLoader<Integer, String>() {
                    @Nullable
                    @Override
                    public String load(@NonNull Integer key) throws Exception {
                        return key + ":value";
                    }
                });

        CompletableFuture completableFuture = cache.get(1);
        Object value = completableFuture.get();
        System.out.println(value);
    }

    /**
     *
     * @param args
     * @throws InterruptedException
     */
    public static void main(String[] args) throws InterruptedException, ExecutionException {
//        handPutCase();
        syncPutCase();
//        asyncPutCase();
    }
}
