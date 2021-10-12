package com.claro.nifi.processors.lookuprecordenrich.interfaces.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.serialization.record.Record;

import com.claro.nifi.processors.lookuprecordenrich.interfaces.IRDBMS;



public class RdbmsImp implements IRDBMS {

	@Override
	public AtomicInteger get(final String query, final DBCPService dbcp, final List<String> lookupValue,
			final HashMap<Integer, String> recordKeys, final HashMap<String, Object> updateRecordPath,
			final Record recordParam, final ComponentLog logger) throws ProcessException, SQLException  {
		
		final AtomicInteger nrOfRows = new AtomicInteger(0);

		try (final Connection conn = dbcp.getConnection();
				final PreparedStatement preparedStatement = conn.prepareStatement(query);) {
		
			recordKeys.forEach((k, v) -> {
				try {
					logger.info("Key: "+ k +" - record.getValue(v):"+ recordParam.getValue(v));
					preparedStatement.setObject(k, recordParam.getValue(v));
				} catch (SQLException ex) {
					logger.error("Failed to process setPreparedStatement {}", new Object[] { ex });
					throw new ProcessException("Failed to process setPreparedStatement ", ex);

				}
			});
			

			try (final ResultSet resultSet = preparedStatement.executeQuery()) {
				while (resultSet.next()) {
					nrOfRows.set(1);
					lookupValue.forEach(x -> {
						try {
							logger.info("Key: "+ x +" - resultSet.getObject(x):"+ resultSet.getObject(x));
							updateRecordPath.put(x, resultSet.getObject(x));
						} catch (SQLException ex) {
							logger.error("Failed to process getResulSet {}", new Object[] { ex });
							throw new ProcessException("Failed to process getResulSet ", ex);
						}
					});
				}
			}
		}

		logger.info("nrOfRows: "+nrOfRows);
		return nrOfRows;
	}

}
