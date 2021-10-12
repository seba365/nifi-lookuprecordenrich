package com.claro.nifi.processors.lookuprecordenrich;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.distributed.cache.client.exception.DeserializationException;
import org.apache.nifi.distributed.cache.client.exception.SerializationException;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.standard.AbstractRouteRecord;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.record.path.RecordPathResult;
import org.apache.nifi.record.path.util.RecordPathCache;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.StringUtils;
import org.apache.nifi.util.Tuple;

import com.claro.nifi.processors.lookuprecordenrich.interfaces.IRDBMS;
import com.claro.nifi.processors.lookuprecordenrich.interfaces.IRedis;
import com.claro.nifi.processors.lookuprecordenrich.interfaces.impl.RdbmsImp;
import com.claro.nifi.processors.lookuprecordenrich.interfaces.impl.RedisImpl;
import com.fasterxml.jackson.databind.ObjectMapper;



@EventDriven
@SideEffectFree
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({ "lookup", "enrichment", "record", "csv", "json", "avro", "logs", "convert", "filter", "jdbc", "cache" })
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({ @ReadsAttribute(attribute = "", description = "") })
@WritesAttributes({ @WritesAttribute(attribute = "", description = "") })
public class LookupRecordEnrich extends AbstractRouteRecord<Tuple<RecordPath, RecordPath>> {
	
	private volatile RecordPathCache recordPathCache;
	private volatile HashMap<Integer, String> recordKeyPaths=new HashMap<>();
	private volatile List<String> recordLookupValue;
	private volatile List<String> recordKeyCache;
	private volatile String sqlQuery;
	private volatile String keyRedis;

	static final Relationship REL_MATCHED = new Relationship.Builder().name("matched")
			.description("All records for which the lookup returns a value will be routed to this relationship")
			.build();
	static final Relationship REL_UNMATCHED = new Relationship.Builder().name("unmatched").description(
			"All records for which the lookup does not have a matching value will be routed to this relationship")
			.build();
	static final Relationship REL_FAILURE = new Relationship.Builder()
	        .name("failure")
	        .description("If a FlowFile cannot be transformed from the configured input format to the configured output format, "
	            + "the unchanged FlowFile will be routed to this relationship")
	        .build();
	
	private static final Set<Relationship> MATCHED_COLLECTION = Collections.singleton(REL_MATCHED);
    private static final Set<Relationship> UNMATCHED_COLLECTION = Collections.singleton(REL_UNMATCHED);
    private static final Set<Relationship> FAILURE_COLLECTION = Collections.singleton(REL_FAILURE);

    
    public static final PropertyDescriptor DBCP_SERVICE = new PropertyDescriptor.Builder()
			.name("Database Connection Pooling Service")
			.description("The Controller Service that is used to obtain connection to database").required(true)
			.identifiesControllerService(DBCPService.class).build();

	public static final PropertyDescriptor SQL_QUERY = new PropertyDescriptor.Builder().name("sql-query")
			.displayName("SQL Query").description("SQL Query").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor LOOKUP_KEY_COLUMN = new PropertyDescriptor.Builder()
			.name("lookup-key-columns").displayName("Lookup Key Columns")
			.description(
					"Campos que se van a utilizar para actualizar el flowfile en base a la consulta a la base de datos. El formato debe ser json")
			.required(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor LOOKUP_VALUE_COLUMN = new PropertyDescriptor.Builder()
			.name("lookup-value-columns").displayName("Lookup Value Columns")
			.description(
					"Campos que se van a utilizar para mostrar los campos de la consulta a la base de datos. Se tienen que separar por una coma")
			.required(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor SEARCH_IN_CACHE = new PropertyDescriptor.Builder().name("search-in-cache")
			.displayName("Search in cache")
			.description("Si es true busca en cache, si es false busca en la base de datos").required(true)
			.allowableValues("true", "false").defaultValue("true").build();

	public static final PropertyDescriptor DISTRIBUTED_CACHE_SERVICE = new PropertyDescriptor.Builder()
			.name("Distributed Cache Service").description("The Controller Service that is used to cache flow files")
			.identifiesControllerService(DistributedMapCacheClient.class).build();

	public static final PropertyDescriptor NAMESPACE_PUT_REDIS = new PropertyDescriptor.Builder()
			.name("Namespace Put Redis").description("Nombre del espacio para el put de Redis")
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).defaultValue("redis:").build();

    
	
	 
    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> propertyDescriptors = new ArrayList<>();
        propertyDescriptors.addAll(super.getSupportedPropertyDescriptors());
    	propertyDescriptors.add(DBCP_SERVICE);
		propertyDescriptors.add(SQL_QUERY);
		propertyDescriptors.add(LOOKUP_KEY_COLUMN);
		propertyDescriptors.add(LOOKUP_VALUE_COLUMN);
		propertyDescriptors.add(SEARCH_IN_CACHE);
		propertyDescriptors.add(DISTRIBUTED_CACHE_SERVICE);
		propertyDescriptors.add(NAMESPACE_PUT_REDIS);
        return propertyDescriptors;
    }
    
    @Override
    public Set<Relationship> getRelationships() {
    	   final Set<Relationship> matchedUnmatchedRels = new HashSet<>();
    	   matchedUnmatchedRels.add(REL_FAILURE);
    	   matchedUnmatchedRels.add(REL_MATCHED);
           matchedUnmatchedRels.add(REL_UNMATCHED);
           
        return matchedUnmatchedRels;
    }
    
	@Override
	protected Set<Relationship> route(Record recordParam, RecordSchema writeSchema, FlowFile flowFile,
			ProcessContext context, Tuple<RecordPath, RecordPath> flowFileContext) {
		
		final ComponentLog logger = getLogger();
		logger.info("record: " + recordParam);
		logger.info("flowFile: " + flowFile);
		logger.info("flowFileContext: " + flowFileContext);
		
		
		final boolean isSearchCache = context.getProperty(SEARCH_IN_CACHE).asBoolean();
		final HashMap<String, Object> updateRecordPath = new HashMap<>();
		final IRDBMS rdbms = new RdbmsImp();
		
		if (isSearchCache) {
			
			if (StringUtils.isBlank(this.keyRedis)) {
				logger.error("Debe tener un namespace para el cache{}", new Object[] { flowFile });
				return UNMATCHED_COLLECTION;
			}
			final DistributedMapCacheClient cacheClient = context.getProperty(DISTRIBUTED_CACHE_SERVICE)
					.asControllerService(DistributedMapCacheClient.class);
			final String keySearch = getKeysCacheConcat(this.keyRedis, recordParam, recordKeyCache, logger);
			final IRedis redis = new RedisImpl();
			
			logger.info("getKeysCacheConcat: " + keySearch);
			try {
				redis.get(cacheClient, keySearch, keySerializer, valueDeserializer).forEach((k, v) -> {
					logger.info("key: " + k + " - value:" + v);
					updateRecordPath.put(k, v);
				});
				
				logger.info("updateRecordPath isEmpty: " + updateRecordPath.isEmpty());
				if (updateRecordPath.isEmpty()) {
					final int rowsCount = getInfoRDBMS(recordParam, context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class), logger, updateRecordPath, rdbms);
					if(rowsCount>0) {
						final String json = new ObjectMapper().writeValueAsString(updateRecordPath);
						redis.put(cacheClient, keySearch, json, keySerializer, keySerializer);
					}else {
						logger.info("No data found - rdbms {}", new Object[] { recordParam });
						return UNMATCHED_COLLECTION;
					}
				}
			} catch (IOException e) {
				logger.error("Failed to process cacheClient {}", new Object[] { isSearchCache, flowFile, e });
				return FAILURE_COLLECTION;
			}
			
		}else {
			final int rowsCount = getInfoRDBMS(recordParam, context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class), logger, updateRecordPath, rdbms);
			if(rowsCount==0) {
				logger.info("No data found - rdbms {}", new Object[] { recordParam });
				return UNMATCHED_COLLECTION;
			}
			
		}

		recordParam.incorporateSchema(writeSchema);

		updateRecordPath.forEach((k, v) -> {
			final String keyPath = "/".concat(k);
			final RecordPath recordPath = recordPathCache.getCompiled(keyPath);
			final RecordPathResult result = recordPath.evaluate(recordParam);
			final Object replacementValue = v;
			result.getSelectedFields().forEach(fieldVal -> fieldVal.updateValue(replacementValue));
		});
		
		 return MATCHED_COLLECTION ;
	}

	private int getInfoRDBMS(Record recordParam, final DBCPService dbcp, final ComponentLog logger,final HashMap<String, Object> updateRecordPath, final IRDBMS rdbms) {
		logger.info("<<<getInfoRDBMS>>>");
		 int rowsCount=0;
		try {
			rowsCount= rdbms.get(sqlQuery, dbcp, recordLookupValue, recordKeyPaths, updateRecordPath,recordParam, logger).get();
			logger.info("rowsCount: "+rowsCount);
		} catch (ProcessException | IllegalArgumentException | SQLException e) {
			logger.error("Failed to process getInfoRDBMS {}", new Object[] { recordParam, e });
			throw new ProcessException("Failed to process getInfoRDBMS ", e);
		}
		return rowsCount;
	}


	@Override
	protected boolean isRouteOriginal() {
			return false;
	}

	@OnScheduled
    public void onScheduled(final ProcessContext context) {
		this.recordKeyCache = Arrays.asList(context.getProperty(LOOKUP_KEY_COLUMN).getValue().trim().split(","));
		this.recordKeyCache.forEach(x -> this.recordKeyPaths.put(this.recordKeyCache.indexOf(x) + 1, x));
		this.recordLookupValue = Arrays.asList(context.getProperty(LOOKUP_VALUE_COLUMN).getValue().trim().split(","));
		this.sqlQuery = context.getProperty(SQL_QUERY).getValue();
		recordPathCache = new RecordPathCache(this.recordLookupValue.size() * 2);
		this.keyRedis = context.getProperty(NAMESPACE_PUT_REDIS).getValue().trim();
	}

	@Override
	protected Tuple<RecordPath, RecordPath> getFlowFileContext(FlowFile flowFile, ProcessContext context) {
		getLogger().info("<<<getFlowFileContext>>>");
		return null;
	}

	
	public String getKeysCacheConcat(final String key, final Record recordParam,final List<String> keyArray,final ComponentLog logger) {
		final List<String> arrKeys = new ArrayList<>(keyArray.size());
		logger.info("getKeysCacheConcat: " + keyArray);
		keyArray.forEach(x -> {
			logger.info("getKeysCacheConcat record: " + recordParam.getValue(x));
			arrKeys.add(recordParam.getValue(x).toString());
		});
		return key.concat(String.join("", arrKeys));
	}	
	

	
	
	
	
	private final Serializer<String> keySerializer = new StringSerializer();

	private final Deserializer<byte[]> valueDeserializer = new ValueDeSerializer();

	public static class StringSerializer implements Serializer<String> {
		@Override
		public void serialize(final String value, final OutputStream out) throws SerializationException, IOException {
			out.write(value.getBytes(StandardCharsets.UTF_8));
		}

	}

	public static class ValueDeSerializer implements Deserializer<byte[]> {

		@Override
		public byte[] deserialize(final byte[] input) throws DeserializationException, IOException {
			if (input == null || input.length == 0) {
				return null;
			}
			return input;
		}
	}

}
