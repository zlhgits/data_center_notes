package com.zlh.bloom.guava;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * 布隆过滤器
 *
 * 简介：本质上布隆过滤器是一种数据结构，比较巧妙的概率型数据结构（probabilistic data structure），特点是高效地插入和查询，
 * 可以用来告诉你 “某样东西一定不存在或者可能存在”
 *
 * 判断一个元素是不是在一个集合里，一般想到的是将所有元素保存起来，然后通过比较来确定。链表、平衡二叉树、散列表，
 * 或者是把元素放到数组或链表里，都是这种思路。以上三种结构的检索时间复杂度分别为O(n), O(logn), O(n/k)，O(n),O(n)。
 * 而布隆过滤器(Bloom Filter)也是用于检索一个元素是否在一个集合中，它的空间复杂度是固定的常数O(m)，
 * 而检索时间复杂度是固定的常数O(k)。相比而言，有1%误报率和最优值k的布隆过滤器，每个元素只需要9.6个比特位--无论元素的大小。
 * 这种优势一方面来自于继承自数组的紧凑性，另外一方面来自于它的概率性质。
 * 1%的误报率通过每个元素增加大约4.8比特，就可以降低10倍
 * @package com.zlh.bloomfilter.guava
 * @company: dacheng
 * @author: zlh
 * @createDate: 2020/7/27
 */
@Slf4j
public class GuavaBloomFielter {
    private static int size = 5000000;

    /**
     * // private static BloomFilter<String> bloomFilter =
     * //
     * funnel: 数据类型漏斗，也是缓存中key的类型
     * size: 预期插入的数量
     * fpp: 期望的误判率
     */
    private volatile BloomFilter<Integer> bloomFilter = BloomFilter.create(Funnels.integerFunnel(), size*2, 0.001);

    public static void main(String[] args) {
        GuavaBloomFielter fielter = new GuavaBloomFielter();
        Long one = System.currentTimeMillis();
//        for (int i = 0; i < size; i++) {
//            fielter.bloomFilter.put(i);
//        }
        Long two = System.currentTimeMillis();
        System.out.println("time: "+(two-one));
        System.out.println("write over!");

//        for (int i = 0; i < size+10; i++) {
//            if (!fielter.bloomFilter.mightContain(i)) {
//                System.err.println("有逃犯越狱了");
//            }
//        }
        Long three = System.currentTimeMillis();
        System.out.println("time: "+(three-two));
        System.out.println("------------------");

        System.out.println(fielter.bloomFilter.approximateElementCount());
        fielter.bloomFilter.put(size+299);
        System.out.println(fielter.bloomFilter.approximateElementCount());
        // 可能存在误判，当布隆过滤器说某个值存在时，这个值可能不存在；当它说不存在时，那就肯定不存在
        List<Integer> list = new ArrayList<>();
        for (int i = size + 10000; i < size + 20000; i++) {
            if (fielter.bloomFilter.mightContain(i)) {
                list.add(i);
            }
        }
        Long fore = System.currentTimeMillis();
        System.out.println("time: "+(fore-three));
        System.out.println("误伤数：" + list.size());
    }
}
