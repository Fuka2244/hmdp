package com.hmdp.service.impl;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

import java.util.List;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_TYPE_KEY;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    //思路：将redis中存入一个json字符串，在数据库中用list来取
    @Override
    public Result queryTypeList() {
        //1.从redis中查询数据:首先接收redis中存取的json字符串
        String shopTypeJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_TYPE_KEY);
        //2.判断是否存在
        if(StrUtil.isNotBlank(shopTypeJson)){
            //3.存在，直接返回,返回的是一个List<ShopType>
            List<ShopType> shopTypeList = JSONUtil.toList(shopTypeJson, ShopType.class);
            return Result.ok(shopTypeList);
        }
        //4.不存在，从数据库中查询数据
        List<ShopType> shopTypeList = list();
        //5.存在，写入redis
        if(CollectionUtil.isNotEmpty(shopTypeList)){
            stringRedisTemplate.opsForValue().set(CACHE_SHOP_TYPE_KEY, JSONUtil.toJsonStr(shopTypeList));

        }
        //返回
        return Result.ok(shopTypeList);
    }
}
