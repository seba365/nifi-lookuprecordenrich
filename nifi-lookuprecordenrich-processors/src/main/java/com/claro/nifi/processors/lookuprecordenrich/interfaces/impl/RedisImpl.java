package com.claro.nifi.processors.lookuprecordenrich.interfaces.impl;

import java.io.IOException;
import java.util.HashMap;


import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.distributed.cache.client.Serializer;

import com.claro.nifi.processors.lookuprecordenrich.interfaces.IRedis;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class RedisImpl implements IRedis {

	@Override
	public void put(DistributedMapCacheClient cache, String key, String value, Serializer<String> keySerialize,
			Serializer<String> valueSerialize) throws IOException {
		cache.put(key, value, keySerialize, valueSerialize);
	}

	@Override
	public HashMap<String, Object> get( DistributedMapCacheClient cache,  String keySearch,
			 Serializer<String> keySerialize,  Deserializer<byte[]> valueDeserialize) throws IOException {
		
		final byte[] byteCacheValues = cache.get(keySearch, keySerialize, valueDeserialize);
				
		return byteCacheValues != null ? new ObjectMapper().readValue(byteCacheValues,
				new TypeReference<HashMap<String, Object>>() {
				}) : new HashMap<>();
	}





}
