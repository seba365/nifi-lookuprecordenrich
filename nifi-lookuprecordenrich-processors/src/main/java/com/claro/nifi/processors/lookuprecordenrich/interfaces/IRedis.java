package com.claro.nifi.processors.lookuprecordenrich.interfaces;

import java.io.IOException;
import java.util.HashMap;

import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.distributed.cache.client.Serializer;

public interface IRedis {
	HashMap<String, Object> get(final DistributedMapCacheClient cache, final String keySearch,
			final Serializer<String> keySerialize, final Deserializer<byte[]> valueDeserialize) throws IOException;

	void put(final DistributedMapCacheClient cache, final String key, final String value,
			final Serializer<String> keySerialize, final Serializer<String> valueSerialize) throws IOException;
}
