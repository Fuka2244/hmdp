package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisData;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private CacheClient cacheClient;

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

    public void saveShop2Redis(Long id,Long expireSeconds){
        //1.查询店铺数据
        Shop shop = getById(id);
        //2.封装逻辑过期时间
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        //3.写入redis
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(redisData));
    }

    @Override
    public Result queryById(Long id) {
        //缓存穿透
        //Shop shop = queryWithPassThrough(id);
//        Shop shop = cacheClient
//                .queryWithPassThrough(id,Shop.class,CACHE_SHOP_KEY,id2->getById(id2),CACHE_SHOP_TTL,TimeUnit.MINUTES);

        //互斥锁解决缓存击穿
//        Shop shop = queryWithMutex(id);
//        if(shop == null){
//            return Result.fail("商铺不存在");
//        }

        //逻辑锁解决缓存击穿
        Shop shop = cacheClient.queryWithLogicalExpire(id, Shop.class, CACHE_SHOP_KEY,this::getById,CACHE_SHOP_TTL,TimeUnit.MINUTES);
        if(shop == null){
            return Result.fail("商铺不存在");
        }
        return Result.ok(shop);
    }

    //线程池
    public static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    //逻辑过期的代码
    public Shop queryWithLogicalExpire(Long id){
        //1.从redis中查询数据
        String shopJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);

        //2.判断是否存在
        if(StrUtil.isBlank(shopJson)){
            //3.不存在，直接返回
           return null;
        }
        //4.命中，需要把json反序列化
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        JSONObject data = (JSONObject) redisData.getData();
        Shop shop = JSONUtil.toBean(data, Shop.class);
        LocalDateTime expireTime = redisData.getExpireTime();
        //5.判断是否过期
        if(expireTime.isAfter(LocalDateTime.now())){
            //5.1未过期，返回店铺信息
            return shop;
        }
        //5.2已过期，需要缓存重建

        //6.缓存重建
        //6.1获取互斥锁
        String lockKEY = LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(lockKEY);
        //6.2判断是否获取成功
        if(!isLock){
            //todo 6.3成功，开启独立现成，实现缓存重建
            CACHE_REBUILD_EXECUTOR.submit(()->{
                //重建缓存
                try {
                    this.saveShop2Redis(id,20L);
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
        return shop;
    }



    //使用互斥锁解决缓存击穿
    public Shop queryWithMutex(Long id){
        //1.从redis中查询数据
        String shopJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);

        //2.判断是否存在
        if(StrUtil.isNotBlank(shopJson)){
            //3.存在，直接返回
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }
        //命中的是否是空值
        if(shopJson!=null){  //下面写的是空字符串"",因此若不是null的话一定是空字符串
            return null;
        }
        //4.实现缓存重建
        //4.1获取互斥锁
        String lockKey = LOCK_SHOP_KEY + id;
        Shop shop = null;
        try {
            boolean isLock = tryLock(lockKey);
            //4.2判断是否获取成功
            if(!isLock){
                //4.3失败，休眠重试
                Thread.sleep(50);
                return queryWithMutex(id);
            }

            //4.4成功，则根据id查询数据库
            shop = getById(id);
            //模拟重建的延迟
            Thread.sleep(200);
            //5.不存在，返回错误
            if(shop == null){
                stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, "",CACHE_NULL_TTL, TimeUnit.MINUTES);
                return null;
            }
            //生成随机ttl事件，防止缓存雪崩
            Long randomTime = ThreadLocalRandom.current().nextLong(MIN_TIME,MAX_TIME);
            //6.存在，写入redis，并返回
            stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(shop),CACHE_SHOP_TTL+randomTime, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }finally {
            //7.释放互斥锁
            unLock(lockKey);
        }
        //返回
        return shop;
    }





    //封装缓存穿透的代码
    public Shop queryWithPassThrough(Long id){
        //1.从redis中查询数据
        String shopJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);

        //2.判断是否存在
        if(StrUtil.isNotBlank(shopJson)){
            //3.存在，直接返回
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }
        //命中的是否是空值
        if(shopJson!=null){  //下面写的是空字符串"",因此若不是null的话一定是空字符串
            return null;
        }
        //4.不存在，从数据库中查询数据
        Shop shop = getById(id);
        //5.不存在，返回错误
        if(shop == null){
            stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, "",CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        //生成随机ttl事件，防止缓存雪崩
        Long randomTime = ThreadLocalRandom.current().nextLong(MIN_TIME,MAX_TIME);
        //6.存在，写入redis，并返回
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(shop),CACHE_SHOP_TTL+randomTime, TimeUnit.MINUTES);
        //返回
        return shop;
    }


    @Override
    @Transactional //开启事务
    public Result update(Shop shop) {
        Long id = shop.getId();
        if(id == null || id <= 0) {
            return Result.fail("商铺id不能为空");
        }
        //1.更新数据库
        updateById(shop);
        //2.删除redis中的数据
        stringRedisTemplate.delete(CACHE_SHOP_KEY + id);
        //3.更新redis中的数据
        return Result.ok();
    }
}
