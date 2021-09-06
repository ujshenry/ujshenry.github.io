package com.henry.db.redis;

import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fiberhome.servicecomponent.util.io.SerializeUtil;

import redis.clients.jedis.BinaryJedisPubSub;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;

public final class JedisUtil {
	private static final Logger LOGGER = LoggerFactory.getLogger(JedisUtil.class);

	private static final int NUM_2000 = 2000;

	private static JedisPool jedisPool = null;

	private static final JedisUtil jedisUtil = new JedisUtil();

	public JedisUtil() {

	}

	static {
		try {
			// 连接池配置项
			JedisPoolConfig config = new JedisPoolConfig();
			// 最大活跃连接数
			config.setMaxTotal(1000);
			// 最大空闲连接数
			config.setMaxIdle(30);
			// 初始化连接数
			config.setMinIdle(30);
			// 最大等待时间
			config.setMaxWaitMillis(1000);
			// 从池中获取连接时，是否进行有效检查
			config.setTestOnBorrow(true);
			// 归还连接时，是否进行有效检查
			config.setTestOnReturn(true);
			// 判断是否需要密码
			if (StringUtils.isNotBlank("XXX")) {
				jedisPool = new JedisPool(config, "XXX.XXX.XXX.XXX", 6379, 100000, "XXX");
			} else {
				jedisPool = new JedisPool(config, "XXX.XXX.XXX.XXX", 6379, 100000);
			}
		} catch (Exception e) {
			LOGGER.error("初始化连接池失败！: " + e);
		}
	}
	
	public static void main(String[] args) {
		JedisUtil jedisUtil = JedisUtil.getInstance();
		System.out.println(jedisUtil.getValue("name"));
		
	}

	/**
	 * ####################################分布式锁开始######################################
	 */
	private static final String LOCK_SUCCESS = "OK";
	private static final String SET_IF_NOT_EXIST = "NX";
	private static final String SET_WITH_EXPIRE_TIME = "PX";
	private static final Long RELEASE_SUCCESS = 1L;

	/**
	 * 尝试获取分布式锁
	 * 
	 * @param lockKey
	 * @param requestId
	 * @param expireTime
	 * @return
	 */
	public boolean tryGetDistributedLock(String lockKey, String requestId, int expireTime) {
		Jedis jedis = getJedis();
		String result = jedis.set(lockKey, requestId, SET_IF_NOT_EXIST, SET_WITH_EXPIRE_TIME, expireTime);
		if (null != jedis) {
			jedis.close();
		}
		if (LOCK_SUCCESS.equals(result)) {
			return true;
		}
		return false;
	}

	/**
	 * 
	 * @param lockKey
	 * @param requestId
	 * @param expireTime
	 */
	public void wrongHetLock(String lockKey, String requestId, int expireTime) {
		Jedis jedis = getJedis();
		Long result = jedis.setnx(lockKey, requestId);
		if (result == 1) {
			jedis.expire(lockKey, expireTime);
		}
	}

	/**
	 * 释放分布式锁
	 * 
	 * @param lockKey
	 * @param requestId
	 * @return
	 */
	public boolean releaseDistributeLock(String lockKey, String requestId) {
		Jedis jedis = getJedis();
		String script = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";
		Object result = jedis.eval(script, Collections.singletonList(lockKey), Collections.singletonList(requestId));
		if (RELEASE_SUCCESS.equals(result)) {
			return true;
		}
		return false;
	}

	/**
	 * ####################################分布式锁结束######################################
	 */

	/**
	 * 从jedis连接池中获取获取jedis对象
	 */
	public Jedis getJedis() {
		Jedis jedis = null;
		try {
			jedis = jedisPool.getResource();
			LOGGER.debug("获取redis连接成功！");
		} catch (Exception e) {
			LOGGER.error("获取redis连接失败！ : " + e.toString());
		}
		return jedis;
	}

	/**
	 * 获取JedisUtil实例
	 *
	 * @return
	 */
	public static JedisUtil getInstance() {
		return jedisUtil;
	}

	/**
	 * 回收jedis(放到finally中)
	 * 
	 * @param jedis
	 */
	public void returnJedis(Jedis jedis) {
		if (null != jedis) {
			jedis.close();
		}
	}

	/**
	 * 取多个集合中的交集
	 * 
	 * @param keys 多个集合的key
	 * @return
	 */
	public Set<String> setInter(String... keys) {
		Jedis jedis = getJedis();
		Set<String> values = jedis.sinter(keys);
		returnJedis(jedis);
		return values;
	}

	/**
	 * 取set集合中的元素数量 set的集合key
	 */
	public long getSetCount(String key) {
		Jedis jedis = getJedis();
		long count = jedis.scard(key);
		returnJedis(jedis);
		return count;
	}

	/**
	 * 获取set集合中的所有元素 set的集合key
	 */
	public Set<String> getSetElements(String key) {
		Jedis jedis = getJedis();
		Set<String> smembers = jedis.smembers(key);
		returnJedis(jedis);
		return smembers;
	}

	/**
	 * 获取set集合中的所有元素 set的集合key
	 */
	public Set<byte[]> getSetElements(byte[] key) {
		Jedis jedis = getJedis();
		Set<byte[]> smembers = jedis.smembers(key);
		returnJedis(jedis);
		return smembers;
	}

	/**
	 * 删除set集合中的某一元素 set的集合key，需删除的元素member
	 */
	public void delSetElements(byte[] key, byte[] member) {
		Jedis jedis = getJedis();
		jedis.srem(key, member);
		returnJedis(jedis);
	}

	/**
	 * 将多个set集合中的交集保存到destKey的目标set中并返回目标结合中的元素集合
	 * 
	 * @param destKey 目标set的key
	 * @param keys    做交集的set的键
	 */
	public Set<String> interToDestKey(String destKey, String... keys) {
		Jedis jedis = getJedis();
		jedis.sinterstore(destKey, keys);
		Set<String> mongoIds = jedis.smembers(destKey);
		returnJedis(jedis);
		return mongoIds;
	}

	/**
	 * 将多个set集合中的并集保存到destKey的目标set中,并返回目标set的元素集合
	 * 
	 * @param destKey 目标set的key
	 * @param keys    做并集的set的键
	 */
	public Set<String> unionToDestKey(String destKey, String... keys) {
		Jedis jedis = getJedis();
		jedis.sunionstore(destKey, keys);
		Set<String> mongoIds = jedis.smembers(destKey);
		returnJedis(jedis);
		return mongoIds;
	}

	/**
	 * 取多个集合中的并集
	 * 
	 * @param keys 多个集合的key
	 * @return
	 */
	public Set<String> setUnion(String... keys) {
		Jedis jedis = getJedis();
		Set<String> values = jedis.sunion(keys);
		returnJedis(jedis);
		return values;
	}

	/**
	 * 将第一个key对多个key做差集后的数据保存到destKey的目标set中,并返回目标set的元素集合
	 * 
	 * @param destKey 目标set的key
	 * @param keys    做差集的set的键
	 */
	public Set<String> diffToDestKey(String destKey, String... keys) {
		Jedis jedis = getJedis();
		jedis.sdiffstore(destKey, keys);
		Set<String> mongoIds = jedis.smembers(destKey);
		returnJedis(jedis);
		return mongoIds;
	}

	/**
	 * 取第一个key对所有key做差集后的元素集合
	 * 
	 * @param keys 多个集合的key
	 * @return
	 */
	public Set<String> setDiff(String... keys) {
		Jedis jedis = getJedis();
		Set<String> values = jedis.sdiff(keys);
		returnJedis(jedis);
		return values;
	}

	/**
	 * 添加元素到有序集合sortedSet
	 *
	 * @param key
	 * @param value
	 * @param score 用于排序
	 */
	public void zadd(String key, String value, double score) {
		Jedis jedis = getJedis();
		jedis.zadd(key, score, value);
		jedis.expire(key, 7200);
		returnJedis(jedis);
	}

	/**
	 * 添加元素到有序集合sortedSet
	 *
	 * @param key
	 * @param value
	 * @param score 用于排序
	 */
	public void zadd(byte[] key, byte[] value, double score) {
		Jedis jedis = getJedis();
		jedis.zadd(key, score, value);
		returnJedis(jedis);
	}

	/**
	 * 添加sorted set
	 *
	 * @param key     sorted的Key名称
	 * @param value   需要添加的值
	 * @param score   需要添加的分值
	 * @param limit   有序集合需要维持的长度
	 * @param reverse true,当队列达到规定的长度时，删除分值最小的值，false删除最大的值
	 */
	public void zaddWithLimit(String key, String value, double score, int limit, boolean reverse) {
		Jedis jedis = getJedis();
		jedis.zadd(key, score, value);
		long totalcount = jedis.zcard(key);
		if (limit < totalcount) {
			if (reverse) {
				jedis.zremrangeByRank(key, 0, 0);
			} else {
				jedis.zremrangeByRank(key, totalcount, totalcount);
			}
		}
		returnJedis(jedis);
	}

	// ↓↓↓↓↓↓↓↓↓↓==K V==↓↓↓↓↓↓↓↓↓↓
	/**
	 * 设置key的值为value
	 * 
	 * @param key
	 * @param value
	 */
	public void setKey(String key, String value) {
		Jedis jedis = getJedis();
		jedis.set(key, value);
		returnJedis(jedis);
	}

	public void setKey(byte[] key, byte[] value) {
		Jedis jedis = getJedis();
		jedis.set(key, value);
		returnJedis(jedis);
	}

	/**
	 * 设置key的值为value，并规定Key的失效时间
	 *
	 * @param key 集合的键 second 过期时间(秒数) vaule 需要加入的值
	 * @return
	 */
	public void setKeyWithExpire(String key, int second, String value) {
		Jedis jedis = getJedis();
		jedis.set(key, value);
		jedis.expire(key, second);
		returnJedis(jedis);
	}

	public void setKeyWithExpire(byte[] key, int second, byte[] value) {
		Jedis jedis = getJedis();
		jedis.set(key, value);
		jedis.expire(key, second);
		returnJedis(jedis);
	}

	public void setKeyWithExpire(String key, int second) {
		Jedis jedis = getJedis();
		jedis.expire(key, second);
		returnJedis(jedis);
	}

	/**
	 * 获取指定key的值
	 *
	 * @param key
	 */
	public String getValue(String key) {
		Jedis jedis = getJedis();
		String value = jedis.get(key);
		returnJedis(jedis);
		return value;
	}

	public byte[] getValue(byte[] key) {
		Jedis jedis = getJedis();
		byte[] value = jedis.get(key);
		returnJedis(jedis);
		return value;
	}
	// ↑↑↑↑↑↑↑↑↑↑==K V==↑↑↑↑↑↑↑↑↑↑

	/**
	 * 获取指定set中的元素集合
	 * 
	 * @param key
	 */
	public Set<String> getSetValue(String key) {
		Jedis jedis = getJedis();
		Set<String> valueSet = jedis.smembers(key);
		returnJedis(jedis);
		return valueSet;
	}

	/**
	 * 返回sortedSet指定范围的集合元素,0为第一个元素，-1为最后一个元素（元素已按由小到大排序）
	 * 
	 * @param key
	 * @param start
	 * @param end
	 * @return
	 */
	public Set<String> zrange(String key, int start, int end) {
		Jedis jedis = getJedis();
		Set<String> set = jedis.zrange(key, start, end);
		returnJedis(jedis);
		return set;
	}

	/**
	 * 获取给定区间的元素，原始按照权重由高到低排序
	 * 
	 * @param key
	 * @param start
	 * @param end
	 * @return
	 */
	public Set<String> zrevrange(String key, int start, int end) {
		Jedis jedis = getJedis();
		Set<String> set = jedis.zrevrange(key, start, end);
		returnJedis(jedis);
		return set;
	}

	/**
	 * 添加对应关系，如果对应关系已存在，则覆盖
	 *
	 * @param key
	 * @param map 对应关系
	 * @return 状态，成功返回OK
	 */
	public String hmset(String key, Map<String, String> map) {
		Jedis jedis = getJedis();
		String s = jedis.hmset(key, map);
		returnJedis(jedis);
		return s;
	}

	/**
	 * 向List尾部追加记录
	 *
	 * @param key
	 * @param value
	 * @return 记录总数
	 */
	public long rpush(String key, String value) {
		Jedis jedis = getJedis();
		long count = jedis.rpush(key, value);
		returnJedis(jedis);
		return count;
	}

	/**
	 * 向List头部追加记录
	 *
	 * @param key
	 * @param value
	 * @return 记录总数
	 */
	public long lpush(String key, String value) {
		Jedis jedis = getJedis();
		long count = jedis.lpush(key, value);
		returnJedis(jedis);
		return count;
	}

	/**
	 * 截取list并返回截取后的元素
	 * 
	 * @param key
	 * @param start
	 * @param end
	 * @return
	 */
	public List<String> ltrim(String key, int start, int end) {
		Jedis jedis = getJedis();
		jedis.ltrim(key, start, end);
		List<String> list = jedis.lrange(key, 0, -1);
		returnJedis(jedis);
		return list;
	}

	/**
	 * 截取list并返回截取后的元素
	 *
	 * @param key
	 * @return
	 */
	public List<String> getListElements(String key) {
		Jedis jedis = getJedis();
		List<String> list = jedis.lrange(key, 0, -1);
		returnJedis(jedis);
		return list;
	}

	/**
	 * 获取list的元素个数
	 * 
	 * @param key
	 * @return
	 */
	public long llen(String key) {
		Jedis jedis = getJedis();
		Long len = jedis.llen(key);
		returnJedis(jedis);
		return len;
	}

	/**
	 * 删除
	 *
	 * @param key
	 * @return 被移除key的数量
	 */
	public long del(String key) {
		Jedis jedis = getJedis();
		long s = jedis.del(key);
		returnJedis(jedis);
		return s;
	}

	public long del(byte[] key) {
		Jedis jedis = getJedis();
		long s = jedis.del(key);
		returnJedis(jedis);
		return s;
	}

	/**
	 * 向set集合中添加集合数据
	 *
	 * @param key    集合的键 vaules 需要加入的值
	 * @param expire 超时时间
	 * @return
	 */
	public void addSetWithExpire(String key, Set<String> vaules, int expire) {
		Jedis jedis = getJedis();
		Pipeline pipeline = jedis.pipelined();
		for (String value : vaules) {
			pipeline.sadd(key, value);
			pipeline.expire(key, expire);
		}
		pipeline.sync();
		returnJedis(jedis);
	}

	/**
	 * 向set集合中添加集合数据，并规定Key的失效时间
	 *
	 * @param key 集合的键 second 过期时间(秒数) vaules 需要加入的值
	 * @return
	 */
	public void updateKeyWithExpire(String key, int second, Set<String> vaules) {
		Jedis jedis = getJedis();
		jedis.del(key);
		Pipeline pipeline = jedis.pipelined();
		for (String value : vaules) {
			pipeline.sadd(key, value);
			pipeline.expire(key, second);
		}
		pipeline.sync();
		returnJedis(jedis);
	}

	/**
	 * 向set集合中添加数据
	 *
	 * @param key 集合的键 vaule 需要加入的值
	 * @return
	 */
	public void addSetValue(String key, String value) {
		Jedis jedis = getJedis();
		jedis.sadd(key, value);
		returnJedis(jedis);
	}

	/**
	 * 向set集合中添加数据
	 *
	 * @param key 集合的键 vaule 需要加入的值
	 * @return
	 */
	public void addSetValue(byte[] key, byte[] value) {
		Jedis jedis = getJedis();
		jedis.sadd(key, value);
		returnJedis(jedis);
	}

	/**
	 * 向set集合中添加数据，并规定Key的失效时间
	 *
	 * @param key 集合的键 second 过期时间(秒数) vaule 需要加入的值
	 * @return
	 */
	public void addSetValueWithExpire(String key, int second, String value) {
		Jedis jedis = getJedis();
		jedis.sadd(key, value);
		jedis.expire(key, second);
		returnJedis(jedis);
	}

	public void addSetValueWithExpire(byte[] key, int second, byte[] value) {
		Jedis jedis = getJedis();
		jedis.sadd(key, value);
		jedis.expire(key, second);
		returnJedis(jedis);
	}

	public void addSetValueWithExpire(String key, int second, String... value) {
		if (null == value || value.length == 0) {
			return;
		}
		Jedis jedis = getJedis();
		jedis.sadd(key, value);
		jedis.expire(key, second);
		returnJedis(jedis);
	}

	/**
	 * 向set集合中添加集合数据，并规定Key的失效时间
	 *
	 * @param valueMap 集合的键 second 过期时间(秒数) vaules 需要加入的值
	 * @return
	 */
	public void addSetWithExpire(Map<String, Set<String>> valueMap, int second) {
		Jedis jedis = getJedis();
		Pipeline pipeline = jedis.pipelined();
		for (Map.Entry<String, Set<String>> entry : valueMap.entrySet()) {
			for (String str : entry.getValue()) {
				pipeline.sadd(entry.getKey(), str);
				pipeline.expire(entry.getKey(), second);
			}
		}
		pipeline.sync();
		returnJedis(jedis);
	}

	/**
	 * 向redis队列中发布消息
	 *
	 * @param channel 通道名
	 * @return
	 */
	public void publishMsg(String channel, Object redisMsg) {
		Jedis jedis = getJedis();
		jedis.publish(channel.getBytes(Charset.forName("UTF-8")), SerializeUtil.serialize(redisMsg));
		returnJedis(jedis);
	}

	/**
	 * 向redis队列中发布消息
	 *
	 * @param channel 通道名
	 * @return
	 */
	public void publishMsg(byte[] channel, byte[] redisMsg) {
		Jedis jedis = getJedis();
		jedis.publish(channel, redisMsg);
		returnJedis(jedis);
	}

	public void subscribeMsg(BinaryJedisPubSub binaryJedisPubSub, String channel) {
		int times = 0;
		long timer = 0L;
		Jedis tmpJedis = null;
		/**
		 * 在ConfigConstant.REDIS_TETRY_EXPIRE_TIME时间内，重试超过ConfigConstant.SUBSCRIBE_RETRY_TIMES次后，结束重试，订阅失败
		 * 否则，在超过ConfigConstant.REDIS_TETRY_EXPIRE_TIME时间后，计数器重置
		 * 避免因网络波动，导致系统重试超过次数后，订阅失效问题
		 **/
		while (true) {
			try {
				if (times == 0) {
					timer = System.currentTimeMillis() + 600000;
				}
				tmpJedis = getJedis();
				LOGGER.info("=================新建了jedis订阅，channel:" + channel + "==================");
				tmpJedis.subscribe(binaryJedisPubSub, channel.getBytes(Charset.forName("UTF-8")));
				returnJedis(tmpJedis);
				break;
			} catch (Exception e) {
				e.printStackTrace();
				LOGGER.warn(String.format("--------subscribe error channelName=【%s】,try times: %s----------", channel, times), e);

				if (times > 5) {
					LOGGER.error("------------redis subscribe times exceed " + 5 + " times,throw Jedis Exception...--------------");
					throw e;
				}
				times++;

				if (timer < System.currentTimeMillis()) {
					LOGGER.warn("---------redis subscribe retry expire " + 600000 + "ms,reset times to zero !---------");
					times = 0;
				}
				try {
					Thread.sleep(NUM_2000);
				} catch (InterruptedException e1) {
					Thread.currentThread().interrupt();
					// ignore
				}
			} finally {
				try {
					if (binaryJedisPubSub.isSubscribed()) {
						binaryJedisPubSub.unsubscribe(channel.getBytes(Charset.forName("UTF-8")));
					}
				} catch (Exception e) {
					LOGGER.error("-------------redis unsubscribe error!-------------", e);
				}
				try {
					returnJedis(tmpJedis);
				} catch (Exception e) {
					LOGGER.error("-------------return redis failed!-------------", e);
				}
			}
		}
	}

//	/**
//	 * 从集合中删除成员
//	 *
//	 * @param key
//	 * @param value
//	 * @return 返回1成功
//	 */
//	public long zrem(String key, String... value) {
//		Jedis jedis = getJedis();
//		long s = jedis.zrem(key, value);
//		returnJedis(jedis);
//		return s;
//	}
//
//	public void saveValueByKey(int dbIndex, byte[] key, byte[] value, int expireTime) throws Exception {
//		Jedis jedis = null;
//		try {
//			jedis = getJedis();
//			jedis.select(dbIndex);
//			jedis.set(key, value);
//			if (expireTime > 0) {
//				jedis.expire(key, expireTime);
//			}
//		} catch (Exception e) {
//			throw e;
//		} finally {
//			returnJedis(jedis);
//		}
//	}
//
//	public byte[] getValueByKey(int dbIndex, byte[] key) throws Exception {
//		Jedis jedis = null;
//		byte[] result = null;
//		try {
//			jedis = getJedis();
//			jedis.select(dbIndex);
//			result = jedis.get(key);
//		} catch (Exception e) {
//			throw e;
//		} finally {
//			returnJedis(jedis);
//		}
//		return result;
//	}
//
//	public void deleteByKey(int dbIndex, byte[] key) throws Exception {
//		Jedis jedis = null;
//		try {
//			jedis = getJedis();
//			jedis.select(dbIndex);
//			jedis.del(key);
//		} catch (Exception e) {
//			throw e;
//		} finally {
//			returnJedis(jedis);
//		}
//	}
//
//	/**
//	 * 获取总数量
//	 *
//	 * @param key
//	 * @return
//	 */
//	public long zcard(String key) {
//		Jedis jedis = getJedis();
//		long count = jedis.zcard(key);
//		returnJedis(jedis);
//		return count;
//	}

	/**
	 * 是否存在KEY
	 * 
	 * @param key
	 * @return
	 */
	public boolean exists(String key) {
		Jedis jedis = getJedis();
		boolean exists = jedis.exists(key);
		returnJedis(jedis);
		return exists;
	}

	public boolean exists(byte[] key) {
		Jedis jedis = getJedis();
		boolean exists = jedis.exists(key);
		returnJedis(jedis);
		return exists;
	}

	/**
	 * 重命名KEY
	 * 
	 * @param oldKey
	 * @param newKey
	 * @return
	 */
	public String rename(String oldKey, String newKey) {
		Jedis jedis = getJedis();
		String result = jedis.rename(oldKey, newKey);
		returnJedis(jedis);
		return result;
	}

	/**
	 * 设置失效时间
	 * 
	 * @param key
	 * @param seconds
	 */
	public void expire(String key, int seconds) {
		Jedis jedis = getJedis();
		jedis.expire(key, seconds);
		returnJedis(jedis);
	}

	public void expire(byte[] key, int seconds) {
		Jedis jedis = getJedis();
		jedis.expire(key, seconds);
		returnJedis(jedis);
	}

	/**
	 * 删除失效时间
	 * 
	 * @param key
	 */
	public void persist(String key) {
		Jedis jedis = getJedis();
		jedis.persist(key);
		returnJedis(jedis);
	}

	public void persist(byte[] key) {
		Jedis jedis = getJedis();
		jedis.persist(key);
		returnJedis(jedis);
	}

	/**
	 * 添加一个键值对，如果键存在不在添加，如果不存在，添加完成以后设置键的有效期
	 * 
	 * @param key
	 * @param value
	 * @param timeOut
	 */
	public void setnxWithTimeOut(String key, String value, int timeOut) {
		Jedis jedis = getJedis();
		if (0 != jedis.setnx(key, value)) {
			jedis.expire(key, timeOut);
		}
		returnJedis(jedis);
	}

	/**
	 * 返回指定key序列值
	 * 
	 * @param key
	 * @return
	 */
	public long incr(String key) {
		Jedis jedis = getJedis();
		long l = jedis.incr(key);
		returnJedis(jedis);
		return l;
	}

	/**
	 * 按照给处的key顺序查看list,并在找到的第一个非空list的尾部弹出一个元素 没有元素时，会阻塞至超时timeout
	 * 
	 * @param timeout
	 * @param keys
	 */
	public List<byte[]> brpop(int timeout, byte[]... keys) {
		Jedis jedis = getJedis();
		List<byte[]> brpop = jedis.brpop(timeout, keys);
		returnJedis(jedis);
		return brpop;
	}

	/**
	 * 按照给处的key顺序查看list,并在找到的第一个非空list的尾部弹出一个元素 没有元素时，会阻塞至超时timeout
	 * 
	 * @param timeout
	 * @param keys
	 */
	public List<String> brpop(int timeout, String... keys) {
		Jedis jedis = getJedis();
		List<String> brpop = jedis.brpop(timeout, keys);
		returnJedis(jedis);
		return brpop;
	}

	public void lpushValue(byte[] key, byte[] messgaes, String taskId) {
		Jedis tmpJedis = getJedis();
		tmpJedis.lpush(key, messgaes);
		tmpJedis.expire(key, 7200);
		tmpJedis.close();
	}

	/**
	 * list添加元素
	 * 
	 * @param key
	 * @param second
	 * @param value
	 */
	public void addListValueWithExpire(String key, int second, String... value) {
		if (null == value || value.length == 0) {
			return;
		}
		Jedis jedis = getJedis();
		jedis.rpush(key, value);
		jedis.expire(key, second);
		returnJedis(jedis);
	}

	/**
	 * 为哈希表中字段赋值，并设置key的过期时间 如果哈希表不存在，一个新的将被创建并进行haset操作
	 * 
	 * @param key    哈希表名
	 * @param field  属性名
	 * @param value  属性值
	 * @param expire 超时时间 单位：秒
	 */
	public void hsetWithExpire(String key, String field, String value, int expire) {
		Jedis jedis = getJedis();
		jedis.hset(key, field, value);
		jedis.expire(key, expire);
		returnJedis(jedis);
	}

	/**
	 * 为哈希表中字段赋值，并设置key的过期时间 如果哈希表不存在，一个新的将被创建并进行haset操作
	 * 
	 * @param key   哈希表名
	 * @param field 属性名
	 * @param value 属性值
	 */
	public void hset(String key, String field, String value) {
		Jedis jedis = getJedis();
		jedis.hset(key, field, value);
		returnJedis(jedis);
	}

	/**
	 * 获取哈希表 key 中，所有的属性名和属性值
	 * 
	 * @param key 哈希表名
	 * @return
	 */
	public Map<String, String> hgetAll(String key) {
		Jedis jedis = getJedis();
		Map<String, String> res = jedis.hgetAll(key);
		returnJedis(jedis);
		return res;
	}

	/**
	 * 获取哈希表 key 中，对应的属性名和属性值
	 * 
	 * @param key   哈希表名
	 * @param field 哈希表名
	 * @return
	 */
	public String hget(String key, String field) {
		Jedis jedis = getJedis();
		String res = jedis.hget(key, field);
		returnJedis(jedis);
		return res;
	}

	/**
	 * 获取哈希表 key 中，所有的属性名和属性值
	 * 
	 * @param key 哈希表名
	 * @return
	 */
	public boolean hexists(String key, String field) {
		Jedis jedis = getJedis();
		boolean res = jedis.hexists(key, field);
		returnJedis(jedis);
		return res;
	}
}
