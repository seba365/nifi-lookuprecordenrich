package com.claro.nifi.processors.lookuprecordenrich.interfaces;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.serialization.record.Record;



public interface IRDBMS {
	AtomicInteger get(final String query, final DBCPService dbcp,final List<String> lookupValue, final HashMap<Integer, String> recordKeys,final HashMap<String, Object> updateRecordPath,final Record recordParam,final ComponentLog logger)throws ProcessException, SQLException;
}
