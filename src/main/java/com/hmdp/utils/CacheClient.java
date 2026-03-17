package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.service.impl.ShopServiceImpl.CACHE_REBUILD_EXECUTOR;
import static com.hmdp.utils.RedisConstants.*;
import static com.hmdp.utils.RedisConstants.CACHE_SHOP_KEY;
import static com.hmdp.utils.RedisConstants.CACHE_SHOP_TTL;
import static com.hmdp.utils.RedisConstants.MAX_TIME;

@Slf4j
@Component
public class CacheClient {
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    //将任意对象写入redis中并设置有效期
    public void set(String key,Object value , Long time , TimeUnit timeUnit){
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value),time,timeUnit);
    }

    //设置逻辑过期的
    public void setWithLogicExpire(String key,Object value , Long time , TimeUnit timeUnit){
        //封装逻辑过期时间
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(timeUnit.toSeconds(time)));

        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    //获得缓存击穿的
    //是任意对象的，因此使用泛型
    public <R,ID> R queryWithPassThrough(ID id , Class<R> type , String keyPrefix, Function<ID,R>dbFallback,Long time , TimeUnit timeUnit){//函数式编程
        String key = keyPrefix + id;
        //1.从redis中查询数据
        String json = stringRedisTemplate.opsForValue().get(key);

        //2.判断是否存在
        if(StrUtil.isNotBlank(json)){
            //3.存在，直接返回
            return JSONUtil.toBean(json,type);
        }
        //命中的是否是空值
        if(json!=null){  //下面写的是空字符串"",因此若不是null的话一定是空字符串
            return null;
        }
        //4.不存在，从数据库中查询数据
        R r = dbFallback.apply(id);
        //5.不存在，返回错误
        if(r == null){
            stringRedisTemplate.opsForValue().set(key, "",time, timeUnit);
            return null;
        }
        //生成随机ttl事件，防止缓存雪崩
        Long randomTime = ThreadLocalRandom.current().nextLong(MIN_TIME,MAX_TIME);
        //6.存在，写入redis，并返回
        //stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(r),time+randomTime, timeUnit);
        this.set(key,r,time+randomTime,timeUnit);
        //返回
        return r;
    }

    //逻辑过期的代码
    public <R,ID>  R queryWithLogicalExpire(ID id, Class<R> type , String keyPrefix, Function<ID,R>dbFallback,Long time , TimeUnit timeUnit){
        //1.从redis中查询数据
        String key = keyPrefix + id;
        String json = stringRedisTemplate.opsForValue().get(key);

        //2.判断是否存在
        if(StrUtil.isBlank(json)){
            //3.不存在，直接返回
            return null;
        }
        //4.命中，需要把json反序列化
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        JSONObject data = (JSONObject) redisData.getData();
        R r = JSONUtil.toBean(data, type);
        LocalDateTime expireTime = redisData.getExpireTime();
        //5.判断是否过期
        if(expireTime.isAfter(LocalDateTime.now())){
            //5.1未过期，返回店铺信息
            return r;
        }
        //5.2已过期，需要缓存重建

        //6.缓存重建
        //6.1获取互斥锁
        String lockKEY = LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(lockKEY);
        //6.2判断是否获取成功
        if(!isLock){
            //6.3成功，开启独立现成，实现缓存重建
            CACHE_REBUILD_EXECUTOR.submit(()->{
                //重建缓存
                try {
                    //查数据库
                    R r1 =dbFallback.apply(id);
                    //写入redis
                    this.setWithLogicExpire(key,r1,time,timeUnit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }finally {
                    //释放锁
                    unLock(lockKEY);
                }
            });

        }

        //6.4失败，返回过期的商铺信息


        //返回
        return r;
    }

    //尝试获取锁
    private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1",10,TimeUnit.SECONDS);
        //不能直接返回，可能会发生拆箱返回空指针
        return BooleanUtil.isTrue(flag);
    }

    //释放锁
    private void unLock(String key){
        stringRedisTemplate.delete(key);
    }



}
