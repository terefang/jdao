/*
 * Copyright (c) 2017. terefang@gmail.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jdao;

import jdao.util.*;

import org.apache.commons.beanutils.BeanUtils;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.dbcp.BasicDataSourceFactory;

import org.apache.commons.dbutils.*;
import org.apache.commons.dbutils.handlers.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.naming.Context;
import javax.naming.InitialContext;

import javax.sql.DataSource;
import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.sql.Connection;

import java.util.*;

public class JDAO
{
	public static Log LOG = LogFactory.getLog(JDAO.class);
	
	public static Map<String, DataSource> scanRecursiveDatasourceDirectories(List<String> configDirs, String suffix)
	{
		Map<String, DataSource> ret = new HashMap();
		try
		{
			for(String scanDirs : configDirs)
			{
				for(String scanDir : scanDirs.split(";"))
				{
					log("INFO: scanning: "+scanDir);
					try
					{
						processDatasourceDirectory(scanDir.trim(), suffix, ret);
					}
					catch(Exception xe)
					{
						log("ERROR: scanning: "+scanDir, xe);
					}
				}
			}
		}
		catch(Exception xe)
		{
			log("ERROR: scanning ds dirs", xe);
		}
		return ret;
	}
	
	
	public static void processDatasourceDirectory(String scanDir, final String suffix, Map<String, DataSource> reg)
	{
		try
		{
			File workDir = new File(scanDir);
			
			if(workDir.isDirectory())
			{
				File[] files = workDir.listFiles(new FileFilter()
				{
					@Override
					public boolean accept(File pathname)
					{
						return pathname.isFile() &&
								pathname.getName().endsWith(suffix);
					}
				});
				
				for(File file : files)
				{
					String dsName = file.getName();
					dsName = dsName.substring(0,dsName.length()-suffix.length());
					DataSource ds = createDatasourceFromFile(file);
					reg.put(dsName, ds);
				}
			}
		}
		catch(Exception xe)
		{
			log("Error processing directory: "+scanDir, xe);
		}
	}
	
	public static DataSource createDatasourceFromFile(File file)
	{
		try
		{
			Properties properties = new Properties();
			FileReader fh = new FileReader(file);
			properties.load(fh);
			fh.close();
			
			DataSource dataSource = null;
			dataSource = createDataSourceByProperties(file, dataSource, properties);
			
			return dataSource;
		}
		catch(Exception xe)
		{
			log("Error processing datasource: "+file, xe);
		}
		return null;
	}
	
	public static DataSource createDataSourceByProperties(File file, DataSource dataSource, Properties properties)
	{
		return createDataSourceByProperties(file.toString(), dataSource, properties);
	}
	
	public static DataSource createDataSourceByProperties(String file, DataSource dataSource, Properties properties)
	{
		try
		{
			if(dataSource==null)
			{
				if(properties.containsKey("dataSourceClassName"))
				{
					dataSource = (DataSource)Thread.currentThread().getContextClassLoader().loadClass(properties.getProperty("dataSourceClassName")).newInstance();
				}
				else
				{
					return (BasicDataSource) BasicDataSourceFactory.createDataSource(properties);
				}
			}
			BeanUtils.populate(dataSource, (Map)properties);
		}
		catch(Exception xe)
		{
			log("Error processing datasource: "+file, xe);
			return null;
		}
		return dataSource;
	}

	public static void log(String text)
	{
		LOG.info(text);
	}
	
	public static void log(String text, Throwable thx)
	{
		LOG.info(text, thx);
	}
	
	
	static GenerousBeanProcessor generousBeanProcessor = new GenerousBeanProcessor();
	static BasicRowProcessor generousRowProcessor = new BasicRowProcessor(generousBeanProcessor);
	static BasicXRowProcessor basicxRowProcessor  = new BasicXRowProcessor();
	static MapListHandler mapListHandler = new MapListHandler(basicxRowProcessor);
	static MapHandler mapHandler = new MapHandler(basicxRowProcessor);
	static ScalarHandler<Object> scalarHandler = new ScalarHandler<Object>();
	static ArrayListHandler arrayListHandler = new ArrayListHandler();
	static ColumnListHandler<Object> columnListHandler = new ColumnListHandler<Object>();
	static KvMapHandler kvMapHandler = new KvMapHandler();
	static KvListMapHandler kvListMapHandler = new KvListMapHandler();
	
	public static JDAO createDaoFromDataSource(DataSource ds, boolean pmt)
			throws Exception
	{
		return new JDAO(new QueryRunner(ds, pmt));
	}
	
	public static JDAO createDaoFromConnection(Connection conn, boolean pmt)
			throws Exception
	{
		return new JDAO(conn, new QueryRunner(pmt));
	}

	public static JDAO createDaoFromJndi(String jndiUri, boolean pmt)
			throws Exception
	{
		return new JDAO(JDAO.lookupDataSourceFromJndi(jndiUri).getConnection(), new QueryRunner(pmt));
	}
	
	/**
	 * Looks for a DataSource in Jndi
	 *
	 * @param  name of the datasource
	 * @return datasource or null
	 */
	public static DataSource lookupDataSourceFromJndi(String name)
			throws Exception
	{
		InitialContext ctx = new InitialContext();
		try
		{
			return lookupDataSourceFromJndi(ctx, name);
		}
		finally
		{
			ctx.close();
		}
	}
	
	public static DataSource lookupDataSourceFromJndi(Context ctx, String name)
			throws Exception
	{
		return(DataSource)ctx.lookup(name);
	}
	
	public static Connection lookupConnectionFromJndi(Context ctx, String name)
			throws Exception
	{
		return lookupDataSourceFromJndi(ctx, name).getConnection();
	}
	
	/**
	 * queries connection according to give dbType and returns data as given by resultsethandler
	 *
	 * @param  dbType, type of database
	 * @param  rsHandler, resultsethandler
	 * @param  conn, database connection
	 * @param  ds, query runner
	 * @param  sql, sql query
	 * @param  args, sql parameters
	 * @return object of type T or null
	 */

	public static <T> T
	queryForT(int dbType, ResultSetHandler<T> rsHandler, Connection conn, QueryRunner ds, String sql, Object... args)
			throws Exception
	{
		if(args == null)
		{
			if(conn==null)
			{
				return ds.query(sql, rsHandler);
			}
			else
			{
				return ds.query(conn, sql, rsHandler);
			}
		}
		else
		if(args.length == 1 && args[0] instanceof Map)
		{
			List nArgs = new Vector();
			sql = preparseParameters(dbType, sql, nArgs, (Map) args[0]);
			if(conn==null)
			{
				return ds.query(sql, rsHandler, nArgs.toArray());
			}
			else
			{
				return ds.query(conn, sql, rsHandler, nArgs.toArray());
			}
		}
		else
		if(args.length > 0 && args[0] instanceof Collection)
		{
			if(conn==null)
			{
				return ds.query(sql, rsHandler, ((Collection)args[0]).toArray());
			}
			else
			{
				return ds.query(conn, sql, rsHandler, ((Collection)args[0]).toArray());
			}
		}
		else
		{
			if(conn==null)
			{
				return ds.query(sql, rsHandler, args);
			}
			else
			{
				return ds.query(conn, sql, rsHandler, args);
			}
		}
	}
	
	public static String join(Collection c, final char separator)
	{
		if (c == null)
		{
			return null;
		}
		
		final StringBuilder buf = new StringBuilder();
		Object[] array = c.toArray();
		int end = array.length;
		for (int i = 0; i < end; i++)
		{
			if (i > 0)
			{
				buf.append(separator);
			}
			if (array[i] != null)
			{
				buf.append(array[i]);
			}
		}
		return buf.toString();
	}
	
	public static <T> T
	queryTemplateForT(int dbType, ResultSetHandler<T> rsHandler, Connection conn, QueryRunner ds, String table, Collection cols, Map<String,String> vm, String suffixQuery, int templateType, int constraintType)
			throws Exception
	{
		List param = new Vector();
		String colString = ((cols == null) ? "*" : JDAO.join(cols, ','));
		return JDAO.queryForT(dbType, rsHandler, conn, ds,  "SELECT "+colString+" FROM "+table+" WHERE "+JDAO.buildWhere(dbType, templateType, constraintType, param, vm)+(suffixQuery==null?"":" "+suffixQuery), param);
	}
	
	public static <T> T
	queryTemplateForT(int dbType, ResultSetHandler<T> rsHandler, Connection conn, QueryRunner ds, String table, Collection cols, Map<String,String> vm, String suffixQuery)
			throws Exception
	{
		return JDAO.queryTemplateForT(dbType, rsHandler, conn, ds, table, cols, vm, suffixQuery, TEMPLATE_TYPE_AUTO, CONSTRAINT_ALL_OF);
	}
	
	public static <T> T
	queryTemplateForT(int dbType, ResultSetHandler<T> rsHandler, Connection conn, QueryRunner ds, String table, Collection cols, Map<String,String> vm)
			throws Exception
	{
		return JDAO.queryTemplateForT(dbType, rsHandler, conn, ds, table, cols, vm, null, TEMPLATE_TYPE_AUTO, CONSTRAINT_ALL_OF);
	}
	
	public static <T> T
	queryTemplateForT(int dbType, ResultSetHandler<T> rsHandler, Connection conn, QueryRunner ds, String table, Map<String,String> vm)
			throws Exception
	{
		return JDAO.queryTemplateForT(dbType, rsHandler, conn, ds, table, null, vm, null, TEMPLATE_TYPE_AUTO, CONSTRAINT_ALL_OF);
	}
	
	/**
	 * executes a query and returns a list of rows (Map)
	 * <p>
	 * if the only (first) argument is a map, the sql string is expected to have "?{field-name}" named parameters
	 * <p>
	 * if the only (first) argument is a list (collection), it is take as the list of arguments.
	 *
	 */
	public static List<Map<String,Object>>
	queryForList(int dbType, Connection conn, QueryRunner ds, String sql, Object... args)
			throws Exception
	{
		return queryForMapList(dbType, conn, ds, sql, args);
	}
	
	public static List<Map<String,Object>>
	queryForList(int dbType, Connection conn, QueryRunner ds, String sql)
			throws Exception
	{
		return queryForMapList(dbType, conn, ds, sql);
	}
	
	public static List<Map<String,Object>>
	queryForMapList(int dbType, Connection conn, QueryRunner ds, String sql, Object... args)
			throws Exception
	{
		return queryForT(dbType, mapListHandler, conn, ds, sql, args);
	}
	
	public static List<Map<String,Object>>
	queryForMapList(int dbType, Connection conn, QueryRunner ds, String sql)
			throws Exception
	{
		return queryForT(dbType, mapListHandler, conn, ds, sql);
	}
	
	public static List<Map<String,Object>>
	queryTemplateForMapList(int dbType, Connection conn, QueryRunner ds, String table, Collection cols, Map vm, String suffixQuery, int templateType, int constraintType)
			throws Exception
	{
		return queryTemplateForT(dbType, mapListHandler, conn, ds, table, cols, vm, suffixQuery, templateType, constraintType);
	}
	
	public static List<Map<String,Object>>
	queryTemplateForMapList(int dbType, Connection conn, QueryRunner ds, String table, Collection cols, Map vm, String suffixQuery)
			throws Exception
	{
		return queryTemplateForT(dbType, mapListHandler, conn, ds, table, cols, vm, suffixQuery, TEMPLATE_TYPE_AUTO, CONSTRAINT_ALL_OF);
	}
	
	public static List<Map<String,Object>>
	queryTemplateForMapList(int dbType, Connection conn, QueryRunner ds, String table, Collection cols, Map vm)
			throws Exception
	{
		return queryTemplateForT(dbType, mapListHandler, conn, ds, table, cols, vm);
	}
	
	public static List<Map<String,Object>>
	queryTemplateForMapList(int dbType, Connection conn, QueryRunner ds, String table, Map vm)
			throws Exception
	{
		return queryTemplateForT(dbType, mapListHandler, conn, ds, table, vm);
	}
	
	/**
	 * executes a query and returns a list of arrays (Object[])
	 * <p>
	 * if the only (first) argument is a map, the sql string is expected to have "?{field-name}" named parameters
	 * <p>
	 * if the only (first) argument is a list (collection), it is take as the list of arguments.
	 *
	 */
	public static List<Object[]> queryForArrayList(int dbType, Connection conn, QueryRunner ds, String sql, Object... args)
			throws Exception
	{
		return queryForT(dbType, arrayListHandler, conn, ds, sql, args);
	}
	
	public static List<Object[]> queryForArrayList(int dbType, Connection conn, QueryRunner ds, String sql)
			throws Exception
	{
		return queryForT(dbType, arrayListHandler, conn, ds, sql);
	}
	
	public static List<Object[]>
	queryTemplateForArrayList(int dbType, Connection conn, QueryRunner ds, String table, Collection cols, Map vm, String suffixQuery, int templateType, int constraintType)
			throws Exception
	{
		return queryTemplateForT(dbType, arrayListHandler, conn, ds, table, cols, vm, suffixQuery, templateType, constraintType);
	}
	
	public static List<Object[]>
	queryTemplateForArrayList(int dbType, Connection conn, QueryRunner ds, String table, Collection cols, Map vm, String suffixQuery)
			throws Exception
	{
		return queryTemplateForT(dbType, arrayListHandler, conn, ds, table, cols, vm, suffixQuery, TEMPLATE_TYPE_AUTO, CONSTRAINT_ALL_OF);
	}
	
	public static List<Object[]>
	queryTemplateForArrayList(int dbType, Connection conn, QueryRunner ds, String table, Collection cols, Map vm)
			throws Exception
	{
		return queryTemplateForT(dbType, arrayListHandler, conn, ds, table, cols, vm);
	}
	
	public static List<Object[]>
	queryTemplateForArrayList(int dbType, Connection conn, QueryRunner ds, String table, Map vm)
			throws Exception
	{
		return queryTemplateForT(dbType, arrayListHandler, conn, ds, table, vm);
	}
	
	/**
	 * executes a query and returns a list of scalars (Object)
	 * <p>
	 * if the only (first) argument is a map, the sql string is expected to have "?{field-name}" named parameters
	 * <p>
	 * if the only (first) argument is a list (collection), it is take as the list of arguments.
	 *
	 */
	public static List<Object> queryForColumnList(int dbType, Connection conn, QueryRunner ds, String sql, Object... args)
			throws Exception
	{
		return queryForT(dbType, columnListHandler, conn, ds, sql, args);
	}
	
	public static List<Object> queryForColumnList(int dbType, Connection conn, QueryRunner ds, String sql)
			throws Exception
	{
		return queryForT(dbType, columnListHandler, conn, ds, sql);
	}
	
	public static List<Object>
	queryTemplateForColumnList(int dbType, Connection conn, QueryRunner ds, String table, String col, Map vm, String suffixQuery, int templateType, int constraintType)
			throws Exception
	{
		return queryTemplateForT(dbType, columnListHandler, conn, ds, table, Collections.singletonList(col), vm, suffixQuery, templateType, constraintType);
	}
	
	public static List<Object>
	queryTemplateForColumnList(int dbType, Connection conn, QueryRunner ds, String table, String col, Map vm, String suffixQuery)
			throws Exception
	{
		return queryTemplateForT(dbType, columnListHandler, conn, ds, table, Collections.singletonList(col), vm, suffixQuery);
	}
	
	public static List<Object>
	queryTemplateForColumnList(int dbType, Connection conn, QueryRunner ds, String table, String col, Map vm)
			throws Exception
	{
		return queryTemplateForT(dbType, columnListHandler, conn, ds, table, Collections.singletonList(col), vm);
	}
	
	/**
	 * executes a query and returns exactly one row (Map)
	 * <p>
	 * if the only (first) argument is a map, the sql string is expected to have "?{field-name}" named parameters
	 * <p>
	 * if the only (first) argument is a list (collection), it is take as the list of arguments.
	 *
	 */
	public static Map<String,Object> queryForMap(int dbType, Connection conn, QueryRunner ds, String sql, Object... args)
			throws Exception
	{
		return queryForT(dbType, mapHandler, conn, ds, sql, args);
	}
	
	public static Map<String,Object> queryForMap(int dbType, Connection conn, QueryRunner ds, String sql)
			throws Exception
	{
		return queryForT(dbType, mapHandler, conn, ds, sql);
	}
	
	public static Map<String,Object>
	queryTemplateForMap(int dbType, Connection conn, QueryRunner ds, String table, Collection cols, Map vm, String suffixQuery, int templateType, int constraintType)
			throws Exception
	{
		return queryTemplateForT(dbType, mapHandler, conn, ds, table, cols, vm, suffixQuery, templateType, constraintType);
	}
	
	public static Map<String,Object>
	queryTemplateForMap(int dbType, Connection conn, QueryRunner ds, String table, Collection cols, Map vm, String suffixQuery)
			throws Exception
	{
		return queryTemplateForT(dbType, mapHandler, conn, ds, table, cols, vm, suffixQuery);
	}
	
	public static Map<String,Object>
	queryTemplateForMap(int dbType, Connection conn, QueryRunner ds, String table, Collection cols, Map vm)
			throws Exception
	{
		return queryTemplateForT(dbType, mapHandler, conn, ds, table, cols, vm);
	}
	
	public static Map<String,Object>
	queryTemplateForMap(int dbType, Connection conn, QueryRunner ds, String table, Map vm)
			throws Exception
	{
		return queryTemplateForT(dbType, mapHandler, conn, ds, table, vm);
	}
	
	public static Map<String,String> queryForKvMap(int dbType, Connection conn, QueryRunner ds, String sql, Object... args)
			throws Exception
	{
		return queryForT(dbType, kvMapHandler, conn, ds, sql, args);
	}
	
	public static Map<String,String> queryForKvMap(int dbType, Connection conn, QueryRunner ds, String sql)
			throws Exception
	{
		return queryForT(dbType, kvMapHandler, conn, ds, sql);
	}
	
	public static Map<String, List<String>> queryForKvListMap(int dbType, Connection conn, QueryRunner ds, String sql, Object... args)
			throws Exception
	{
		return queryForT(dbType, kvListMapHandler, conn, ds, sql, args);
	}
	
	public static Map<String, List<String>> queryForKvListMap(int dbType, Connection conn, QueryRunner ds, String sql)
			throws Exception
	{
		return queryForT(dbType, kvListMapHandler, conn, ds, sql);
	}
	
	public static <T> Map<String,T> queryForStringBeanMap(int dbType, Connection conn, QueryRunner ds, String sql, Class<T> beanClazz, Object... args)
			throws Exception
	{
		return queryForBeanMap(dbType, conn, ds, sql, String.class, beanClazz, args);
	}
	
	public static <T> Map<String,T> queryForStringBeanMap(int dbType, Connection conn, QueryRunner ds, String sql, Class<T> beanClazz)
			throws Exception
	{
		return queryForBeanMap(dbType, conn, ds, sql, String.class, beanClazz);
	}
	
	public static <K,V> Map<K,V> queryForBeanMap(int dbType, Connection conn, QueryRunner ds, String sql, Class<K> idClazz, Class<V> beanClazz, Object... args)
			throws Exception
	{
		BeanMapHandler<K, V> handler = new BeanMapHandler<K, V>(beanClazz, generousRowProcessor);
		
		return queryForT(dbType, handler, conn, ds, sql, args);
	}
	
	public static <K,V> Map<K,V> queryForBeanMap(int dbType, Connection conn, QueryRunner ds, String sql, Class<K> idClazz, Class<V> beanClazz)
			throws Exception
	{
		BeanMapHandler<K, V> handler = new BeanMapHandler<K, V>(beanClazz, generousRowProcessor);
		
		return queryForT(dbType, handler, conn, ds, sql);
	}
	
	public static <T> List<T> queryForBeanList(int dbType, Connection conn, QueryRunner ds, String sql, Class<T> beanClazz, Object... args)
			throws Exception
	{
		ResultSetHandler<List<T>> handler = new BeanListHandler<T>(beanClazz, generousRowProcessor);
		
		return queryForT(dbType, handler, conn, ds, sql, args);
	}
	
	public static <T> List<T> queryForBeanList(int dbType, Connection conn, QueryRunner ds, String sql, Class<T> beanClazz)
			throws Exception
	{
		ResultSetHandler<List<T>> handler = new BeanListHandler<T>(beanClazz, generousRowProcessor);
		
		return queryForT(dbType, handler, conn, ds, sql);
	}
	
	public static <T> T queryForBean(int dbType, Connection conn, QueryRunner ds, String sql, Class<T> beanClazz, Object... args)
			throws Exception
	{
		ResultSetHandler<T> handler = new BeanHandler<T>(beanClazz, generousRowProcessor);
		
		return queryForT(dbType, handler, conn, ds, sql, args);
	}
	
	public static <T> T queryForBean(int dbType, Connection conn, QueryRunner ds, String sql, Class<T> beanClazz)
			throws Exception
	{
		ResultSetHandler<T> handler = new BeanHandler<T>(beanClazz, generousRowProcessor);
		
		return queryForT(dbType, handler, conn, ds, sql);
	}
	
	/**
	 * executes a query and returns exactly one Object
	 * <p>
	 * if the only (first) argument is a map, the sql string is expected to have "?{field-name}" named parameters
	 * <p>
	 * if the only (first) argument is a list (collection), it is take as the list of arguments.
	 *
	 */
	public static Object queryForScalar(int dbType, Connection conn, QueryRunner ds, String sql, Object... args)
			throws Exception
	{
		return queryForT(dbType, scalarHandler, conn, ds, sql, args);
	}
	
	public static Object queryForScalar(int dbType, Connection conn, QueryRunner ds, String sql)
			throws Exception
	{
		return queryForT(dbType, scalarHandler, conn, ds, sql);
	}
	
	public static Object
	queryTemplateForScalar(int dbType, Connection conn, QueryRunner ds, String table, String col, Map vm, String suffixQuery, int templateType, int constraintType)
			throws Exception
	{
		return queryTemplateForT(dbType, scalarHandler, conn, ds, table, Collections.singletonList(col), vm, suffixQuery, templateType, constraintType);
	}
	
	public static Object
	queryTemplateForScalar(int dbType, Connection conn, QueryRunner ds, String table, String col, Map vm, String suffixQuery)
			throws Exception
	{
		return queryTemplateForT(dbType, scalarHandler, conn, ds, table, Collections.singletonList(col), vm, suffixQuery);
	}
	
	public static Object
	queryTemplateForScalar(int dbType, Connection conn, QueryRunner ds, String table, String col, Map vm)
			throws Exception
	{
		return queryTemplateForT(dbType, scalarHandler, conn, ds, table, Collections.singletonList(col), vm);
	}
	
	/**
	 * executes a statment and returns the number of rows effected.
	 * <p>
	 * if the only (first) argument is a map, the sql string is expected to have "?{field-name}" named parameters
	 * <p>
	 * if the only (first) argument is a list (collection), it is take as the list of arguments.
	 *
	 */
	public static int update(int dbType, Connection conn, QueryRunner ds, String sql, Object... args)
			throws Exception
	{
		if(args == null)
		{
			if(conn==null)
			{
				return ds.update(sql);
			}
			else
			{
				return ds.update(conn, sql);
			}
		}
		else
		if(args.length >0 && args[0] instanceof Map)
		{
			List nArgs = new Vector();
			sql = preparseParameters(dbType, sql, nArgs, (Map) args[0]);
			if(conn==null)
			{
				return ds.update(sql, nArgs.toArray());
			}
			else
			{
				return ds.update(conn, sql, nArgs.toArray());
			}
		}
		else
		if(args.length >0 && args[0] instanceof Collection)
		{
			if(conn==null)
			{
				return ds.update(sql, ((Collection)args[0]).toArray());
			}
			else
			{
				return ds.update(conn, sql, ((Collection)args[0]).toArray());
			}
		}
		else
		{
			if(conn==null)
			{
				return ds.update(sql, args);
			}
			else
			{
				return ds.update(conn, sql, args);
			}
		}
	}
	
	/**
	 * executes a statment and returns the number of rows effected.
	 * <p>
	 * if the only (first) argument is a map, the sql string is expected to have "?{field-name}" named parameters
	 * <p>
	 * if the only (first) argument is a list (collection), it is take as the list of arguments.
	 *
	 */
	public static int execute(int dbType, Connection conn, QueryRunner ds, String sql, Object... args)
			throws Exception
	{
		return update(dbType, conn, ds, sql, args);
	}
	
	public static int execute(int dbType, Connection conn, QueryRunner ds, String sql)
			throws Exception
	{
		return update(dbType, conn, ds, sql);
	}
	
	/**
	 * executes an insert (mysql-insert-set) with optional on-duplicate-key-update and returns the number of rows effected.
	 *
	 * @return nrows
	 */
	public static int insertSet(int dbType, Connection conn, QueryRunner ds, String table, Map cols, boolean onDuplicateKeyUpdate)
			throws Exception
	{
		return insertSet(dbType, conn, ds, table, cols, onDuplicateKeyUpdate, cols.keySet());
	}
	
	public static int insertSet(int dbType, Connection conn, QueryRunner ds, String table, Map cols, boolean onDuplicateKeyUpdate, Collection updateFields)
			throws Exception
	{
		if(dbType != JDAO.DB_TYPE_MYSQL)
		{
			throw new IllegalArgumentException("DB TYPE NOT MYSQL");
		}
		Vector vv = new Vector();
		String setqq = buildSet(dbType, vv, cols);
		StringBuilder qq=new StringBuilder();
		qq.append("INSERT INTO "+table+" SET ");
		qq.append(setqq);
		if(onDuplicateKeyUpdate)
		{
			Map um =new HashMap();
			for(Object o : updateFields)
			{
				um.put(o, cols.get(o));
			}
			String setuqq = buildSet(dbType, vv, um);
			qq.append(" ON DUPLICATE KEY UPDATE ");
			qq.append(setuqq);
		}
		
		if(conn==null)
		{
			return ds.update(qq.toString(), vv.toArray());
		}
		return ds.update(conn, qq.toString(), vv.toArray());
	}
	
	/**
	 * executes an insert and returns the number of rows effected.
	 *
	 */
	public static int insert(int dbType, Connection conn, QueryRunner ds, String table, Map cols)
			throws Exception
	{
		return insert(dbType, conn, ds, table, cols, false);
	}
	
	public static int insert(int dbType, Connection conn, QueryRunner ds, String table, Map cols, boolean onDuplicateKeyUpdate)
			throws Exception
	{
		return insert(dbType, conn, ds, table, cols, onDuplicateKeyUpdate, cols.keySet());
	}
	
	public static int insert(int dbType, Connection conn, QueryRunner ds, String table, Map cols, boolean onDuplicateKeyUpdate, Collection updateFields)
			throws Exception
	{
		if(onDuplicateKeyUpdate && (dbType != JDAO.DB_TYPE_MYSQL))
		{
			throw new IllegalArgumentException("DB TYPE NOT MYSQL");
		}
		Vector parm = new Vector();
		StringBuilder qq=new StringBuilder();
		qq.append("INSERT INTO "+table+" ( ");
		boolean op = true;
		for(Object kv : cols.entrySet())
		{
			parm.add(((Map.Entry)kv).getValue());
			if(!op)
			{
				qq.append(",");
			}
			qq.append(((Map.Entry)kv).getKey());
			op=false;
		}
		qq.append(" ) VALUES (");
		op = true;
		for(Object v : parm)
		{
			if(!op)
			{
				qq.append(",");
			}
			qq.append("?");
			op=false;
		}
		qq.append(" ) ");
		
		if(onDuplicateKeyUpdate)
		{
			Map um =new HashMap();
			for(Object o : updateFields)
			{
				um.put(o, cols.get(o));
			}
			String setuqq = buildSet(dbType, parm, um);
			qq.append(" ON DUPLICATE KEY UPDATE ");
			qq.append(setuqq);
		}
		
		if(conn==null)
		{
			return ds.update(qq.toString(), parm.toArray());
		}
		return ds.update(conn, qq.toString(), parm.toArray());
	}
	
	/**
	 * create a set statement-fragment and parameter-list from a column-map.
	 *
	 */
	public static String buildSet(int dbType, List parm, Map vm)
	{
		StringBuilder qq=new StringBuilder();
		boolean op = true;
		for(Object kv : vm.entrySet())
		{
			String k = ((Map.Entry)kv).getKey().toString();
			Object v = ((Map.Entry)kv).getValue();
			if(op == true)
			{
				qq.append(k+"=?");
				op = false;
			}
			else
			{
				qq.append(", "+k+"=?");
			}
			parm.add(v);
		}
		return(qq.toString());
	}
	
	
	public static final int TEMPLATE_TYPE_AUTO = 0;
	public static final int TEMPLATE_TYPE_EQUAL = 1;
	public static final int TEMPLATE_TYPE_NOT_EQUAL = 2;
	public static final int TEMPLATE_TYPE_SUBSTRING = 3;
	public static final int TEMPLATE_TYPE_STARTSWITH = 4;
	public static final int TEMPLATE_TYPE_LIKE = 5;
	public static final int TEMPLATE_TYPE_REGEX = 6;
	
	public static final int CONSTRAINT_ANY_OF = 0;
	public static final int CONSTRAINT_ALL_OF = 1;
	
	/**
	 * create a where statement-fragment and parameter-list from a column-map and constraint-type based on LIKE.
	 *
	 */
	public static String buildWhereLike(int dbType, int constraintType, List param, Map<String,String> vm)
	{
		StringBuilder sb = new StringBuilder();
		boolean first = true;
		int pNum = param.size();
		
		if(vm.keySet().size() > 0)
		{
			sb.append(" ( ");
			for(String k : vm.keySet())
			{
				String v = vm.get(k);
				if(v!="" && v!="*" && v!="%")
				{
					if(first)
					{
						sb.append(" ("+likeOpPerDbType(dbType, k, "?", false)+")");
					}
					else if(constraintType==CONSTRAINT_ALL_OF)
					{
						sb.append(" AND ("+likeOpPerDbType(dbType, k, "?", false)+")");
					}
					else if(constraintType==CONSTRAINT_ANY_OF)
					{
						sb.append(" OR ("+likeOpPerDbType(dbType, k, "?", false)+")");
					}
					else
					{
						sb.append(" OR ("+likeOpPerDbType(dbType, k, "?", false)+")");
					}
					param.add(v);
					first=false;
				}
			}
			sb.append(" ) ");
		}
		
		if(pNum == param.size())
		{
			return " TRUE ";
		}
		
		return(sb.toString());
	}

	public static String likeOpPerDbType(int dbType, String arg1, String arg2, boolean invert)
	{
		switch(dbType)
		{
			case DB_TYPE_POSTGRES:
			{
				return arg1+(invert?" NOT":"")+" ILIKE "+arg2;
			}
			case DB_TYPE_ORACLE:
			case DB_TYPE_MSSQL:
			{
				return "LOWER("+arg1+")"+(invert?" NOT":"")+" LIKE "+arg2;
			}
			case DB_TYPE_ANSI:
			case DB_TYPE_MYSQL:
			case DB_TYPE_SYBASE:
			case DB_TYPE_DB2:
			case DB_TYPE_H2:
			case DB_TYPE_SQLITE:
			case DB_TYPE_CRATE:
			default:
			{
				return arg1+(invert?" NOT":"")+" LIKE "+arg2;
			}
		}
	}
	
	public static String regexpOpPerDbType(int dbType, String arg1, String arg2, boolean invert)
	{
		switch(dbType)
		{
			case DB_TYPE_POSTGRES:
			{
				return arg1+(invert?" !":" ")+"~* "+arg2;
			}
			case DB_TYPE_ORACLE:
			{
				return (invert?"NOT ":"")+"REGEXP_LIKE("+arg1+", "+arg2+", 'i')";
			}
			case DB_TYPE_MYSQL:
			{
				return arg1+(invert?" NOT":"")+" RLIKE "+arg2;
			}
			case DB_TYPE_MSSQL:
			case DB_TYPE_SYBASE:
			case DB_TYPE_DB2:
			case DB_TYPE_H2:
			case DB_TYPE_SQLITE:
			case DB_TYPE_CRATE:
			case DB_TYPE_ANSI:
			default:
			{
				return arg1+(invert?" NOT":"")+" REGEXP "+arg2;
			}
		}
	}
	/**
	 * create a where statement-fragment and parameter-list from a column-map and constraint-type based on REGEXP.
	 *
	 */
	public static String buildWhereRegexp(int dbType, int constraintType, List param, Map<String,String> vm)
	{
		StringBuilder sb = new StringBuilder();
		boolean first = true;
		int pNum = param.size();
		
		if(vm.keySet().size() > 0)
		{
			sb.append(" ( ");
			for(String k : vm.keySet())
			{
				String v = vm.get(k);
				if(v!="" && v!="*" && v!=".*")
				{
					if(first)
					{
						sb.append(" ("+regexpOpPerDbType(dbType, k, "?", false)+")");
					}
					else if(constraintType==CONSTRAINT_ALL_OF)
					{
						sb.append(" AND ("+regexpOpPerDbType(dbType, k, "?", false)+")");
					}
					else if(constraintType==CONSTRAINT_ANY_OF)
					{
						sb.append(" OR ("+regexpOpPerDbType(dbType, k, "?", false)+")");
					}
					else
					{
						sb.append(" OR ("+regexpOpPerDbType(dbType, k, "?", false)+")");
					}
					param.add(v);
					first=false;
				}
			}
			sb.append(" ) ");
		}
		
		if(pNum == param.size())
		{
			return " TRUE ";
		}
		
		return(sb.toString());
	}
	
	
	/**
	 * create a where statement-fragment and parameter-list from a column-map, template-type and constraint-type.
	 *
	 */
	public static String buildWhere(int dbType, int templateType, int constraintType, List param, Map<String,String> template)
	{
		switch(templateType)
		{
			case TEMPLATE_TYPE_AUTO:
				return buildWhereAuto(dbType, param, template);
			case TEMPLATE_TYPE_EQUAL:
				return buildWhereEqual(dbType, param, template);
			case TEMPLATE_TYPE_NOT_EQUAL:
				return buildWhereNotEqual(dbType, param, template);
			case TEMPLATE_TYPE_LIKE:
				return buildWhereLike(dbType, constraintType, param, template);
			case TEMPLATE_TYPE_REGEX:
				return buildWhereRegexp(dbType, constraintType, param, template);
			case TEMPLATE_TYPE_STARTSWITH:
				return buildWherePrefix(dbType, param, template);
			case TEMPLATE_TYPE_SUBSTRING:
				return buildWhereSubstr(dbType, param, template);
			default:
				return buildWhereLike(dbType, constraintType, param, template);
		}
	}
	
	
	public static void parseSpec(int dbType, StringBuilder sb, List param, String k, String s)
	{
		if(s==null || s.trim().length()==0)
		{
			sb.append(" TRUE ");
			return;
		}
		boolean invert = false;
		
		if(s.charAt(0)=='!')
		{
			invert = true;
			s = s.substring(1);
		}
		
		if(s.charAt(0)=='+' || s.charAt(0)=='-')
		{
			String[] list = s.split("[,;]");
			
			sb.append(" ("+(invert ? " TRUE":" FALSE"));
			
			for(String item : list)
			{
				sb.append(invert ? " AND": " OR");
				if(s.charAt(0)=='+')
				{
					parseSpec_(dbType, sb, param, k, item.substring(1), invert);
				}
				else if(s.charAt(0)=='-')
				{
					parseSpec_(dbType, sb, param, k, item.substring(1), !invert);
				}
				else
				{
					parseSpec_(dbType, sb, param, k, item, invert);
				}
			}
			
			sb.append(")");
		}
		else
		{
			parseSpec_(dbType, sb, param, k, s, invert);
		}
	}
	
	static void parseSpec_(int dbType, StringBuilder sb, List param, String k, String s, boolean invert)
	{
		if(s.trim().length()==0)
		{
			return;
		}
		
		if(s.charAt(0)=='~')
		{
			s=s.substring(1);
			sb.append(" ("+regexpOpPerDbType(dbType, k, "?", invert)+")");
		}
		else if(s.charAt(0)=='^')
		{
			s=s.substring(1);
			sb.append(" ("+likeOpPerDbType(dbType, k, "?", invert)+")");
			param.add(s+'%');
			return;
		}
		else if(s.indexOf('*')>=0)
		{
			s=s.replace('*', '%');
			sb.append(" ("+likeOpPerDbType(dbType, k, "?", invert)+")");
		}
		else if(s.indexOf('%')>=0)
		{
			sb.append(" ("+likeOpPerDbType(dbType, k, "?", invert)+")");
		}
		else
		{
			if(invert)
			{
				sb.append(" ("+k+"!=?)");
			}
			else
			{
				sb.append(" ("+k+"=?)");
			}
		}
		param.add(s);
	}
	
	
	public static String buildWhereAuto(int dbType, List param, Map<String,String> vm)
	{
		StringBuilder sb = new StringBuilder();
		
		sb.append(" TRUE ");
		for(String k : vm.keySet())
		{
			String v = vm.get(k);
			if(v!="")
			{
				sb.append(" AND ");
				parseSpec(dbType, sb, param, k, v);
			}
		}
		return(sb.toString());
	}
	
	public static String buildWhereEqual(int dbType, List param, Map<String,String> vm)
	{
		StringBuilder sb = new StringBuilder();
		
		sb.append(" TRUE ");
		for(String k : vm.keySet())
		{
			String v = vm.get(k);
			if(v!="")
			{
				sb.append(" AND ("+k+" = ?)");
				param.add(v);
			}
		}
		return(sb.toString());
	}
	
	public static String buildWhereNotEqual(int dbType, List param, Map<String,String> vm)
	{
		StringBuilder sb = new StringBuilder();
		
		sb.append(" TRUE ");
		for(String k : vm.keySet())
		{
			String v = vm.get(k);
			if(v!="")
			{
				sb.append(" AND ("+k+" != ?)");
				param.add(v);
			}
		}
		return(sb.toString());
	}
	
	public static String buildWhereSubstr(int dbType, List param, Map<String,String> vm)
	{
		StringBuilder sb = new StringBuilder();
		
		sb.append(" TRUE ");
		for(String k : vm.keySet())
		{
			String v = vm.get(k);
			if(v!="")
			{
				sb.append(" AND ("+likeOpPerDbType(dbType, k, "?", false)+")");
				param.add("%"+v+"%");
			}
		}
		return(sb.toString());
	}
	
	public static String buildWherePrefix(int dbType, List param, Map<String,String> vm)
	{
		StringBuilder sb = new StringBuilder();
		
		sb.append(" TRUE ");
		for(String k : vm.keySet())
		{
			String v = vm.get(k);
			if(v!="")
			{
				sb.append(" AND ("+likeOpPerDbType(dbType, k, "?", false)+")");
				param.add(v+"%");
			}
		}
		return(sb.toString());
	}
	
	public static String preparseParameters(int dbType, String format, List param, Map vars)
	{
		String prefix = "?{";
		String suffix = "}";
		StringBuilder sb = new StringBuilder();
		
		int offset = 0;
		int found = -1;
		while((found = format.indexOf(prefix, offset)) >= offset)
		{
			sb.append(format.substring(offset, found));
			
			if(suffix.length()==0)
			{
				offset = found+prefix.length()+1;
			}
			else
			{
				offset = format.indexOf(suffix, found+prefix.length());
			}
			
			if(offset > found)
			{
				String tag = format.substring(found+prefix.length(), offset);
				offset += suffix.length();
				
				sb.append("?");
				if(vars.containsKey(tag))
				{
					param.add(vars.get(tag));
				}
				else
				if(vars.containsKey(tag.toLowerCase()))
				{
					param.add(vars.get(tag.toLowerCase()));
				}
				else
				if(vars.containsKey(tag.toUpperCase()))
				{
					param.add(vars.get(tag.toUpperCase()));
				}
				else
				{
					param.add("{"+tag.toUpperCase()+"}");
				}
			}
			else
			{
				sb.append(prefix);
				offset = found+prefix.length();
			}
		}
		sb.append(format.substring(offset));
		
		return sb.toString();
	}
	
	
	public static List<String> queryFieldList(int dbType, Connection conn, QueryRunner ds, String schemaName, String tableName) throws Exception
	{
		switch(dbType)
		{
			case DB_TYPE_POSTGRES:
			case DB_TYPE_ORACLE:
			case DB_TYPE_MSSQL:
			case DB_TYPE_SYBASE:
			case DB_TYPE_DB2:
			case DB_TYPE_H2:
			case DB_TYPE_SQLITE:
			case DB_TYPE_CRATE:
			{
				return (List)queryForColumnList(dbType, conn, ds, "SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_NAME=? AND TABLE_SCHEMA=? ", tableName, schemaName);
			}
			case DB_TYPE_ANSI:
			case DB_TYPE_MYSQL:
			default:
			{
				return (List)queryForColumnList(dbType, conn, ds, "SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_NAME=? AND TABLE_SCHEMA=? ORDER BY ORDINAL_POSITION", tableName, schemaName);
			}
		}
	}
	
	public static Map<String,Object> filterFields(Map<String,Object> row, List<String> fieldList) throws Exception
	{
		LinkedHashMap<String,Object> returnRow = new LinkedHashMap();
		for(String key : fieldList)
		{
			if(row.containsKey(key))
			{
				returnRow.put(key, row.get(key));
			}
		}
		return returnRow;
	}
	
	QueryRunner queryRunner = null;
	Connection conn = null;
	
	public int getDbType()
	{
		return dbType;
	}
	
	public void setDbType(int dbType)
	{
		this.dbType = dbType;
	}
	
	int dbType = 0;
	
	public static final int DB_TYPE_ANSI = 0;
	public static final int DB_TYPE_MYSQL = 1;
	public static final int DB_TYPE_ORACLE = 2;
	public static final int DB_TYPE_POSTGRES = 3;
	public static final int DB_TYPE_MSSQL = 4;
	public static final int DB_TYPE_SYBASE = 5;
	public static final int DB_TYPE_DB2 = 6;
	public static final int DB_TYPE_H2 = 7;
	public static final int DB_TYPE_SQLITE = 8;
	public static final int DB_TYPE_CRATE = 9;
	
	public JDAO() { }
	
	public JDAO(QueryRunner queryRunner)
	{
		this();
		this.queryRunner = queryRunner;
		
	}
	
	public JDAO(Connection conn, QueryRunner queryRunner)
	{
		this(queryRunner);
		this.conn = conn;
	}
	
	public  List<Map<String,Object>>
	queryForList(String sql, Object... args)
			throws Exception
	{
		return JDAO.queryForList(this.dbType, this.conn, this.queryRunner, sql, args);
	}
	
	public  List<Map<String,Object>>
	queryForList(String sql)
			throws Exception
	{
		return JDAO.queryForList(this.dbType, this.conn, this.queryRunner, sql);
	}
	
	public  List<Map<String,Object>>
	queryForMapList(String sql, Object... args)
			throws Exception
	{
		return JDAO.queryForMapList(this.dbType, this.conn, this.queryRunner, sql, args);
	}
	
	public  List<Map<String,Object>>
	queryForMapList(String sql)
			throws Exception
	{
		return JDAO.queryForMapList(this.dbType, this.conn, this.queryRunner, sql);
	}
	
	public List<Map<String, Object>>
	queryTemplateForMapList(String table, Map<String,String> vm, Collection fieldList, String suffixQuery)
			throws Exception
	{
		return JDAO.queryTemplateForMapList(this.dbType, this.conn, this.queryRunner,  table, fieldList, vm, suffixQuery);
	}
	
	public List<Map<String, Object>>
	queryTemplateForMapList(String table, Map<String,String> vm, Collection fieldList)
			throws Exception
	{
		return JDAO.queryTemplateForMapList(this.dbType, this.conn, this.queryRunner,  table, fieldList, vm);
	}
	
	public List<Map<String, Object>>
	queryTemplateForMapList(String table, Map<String,String> vm)
			throws Exception
	{
		return JDAO.queryTemplateForMapList(this.dbType, this.conn, this.queryRunner,  table, vm);
	}
	
	public  List<Object[]> queryForArrayList(String sql, Object... args)
			throws Exception
	{
		return JDAO.queryForArrayList(this.dbType, this.conn, this.queryRunner, sql, args);
	}
	
	public  List<Object[]> queryForArrayList(String sql)
			throws Exception
	{
		return JDAO.queryForArrayList(this.dbType, this.conn, this.queryRunner, sql);
	}
	
	public List<Object[]>
	queryTemplateForArrayList(String table, List<String> cols, Map<String,String> vm, String suffixQuery)
			throws Exception
	{
		return JDAO.queryTemplateForArrayList(this.dbType, this.conn, this.queryRunner, table, cols, vm, suffixQuery);
	}
	
	public List<Object[]>
	queryTemplateForArrayList(String table, List<String> cols, Map<String,String> vm)
			throws Exception
	{
		return JDAO.queryTemplateForArrayList(this.dbType, this.conn, this.queryRunner, table, cols, vm);
	}
	
	public  List<Object> queryForColumnList(String sql, Object... args)
			throws Exception
	{
		return JDAO.queryForColumnList(this.dbType, this.conn, this.queryRunner, sql, args);
	}
	
	public  List<Object> queryForColumnList(String sql)
			throws Exception
	{
		return JDAO.queryForColumnList(this.dbType, this.conn, this.queryRunner, sql);
	}
	
	public List<Object>
	queryTemplateForColumnList(String table, String col, Map<String,String> vm, String suffixQuery, int templateType, int constraintType)
			throws Exception
	{
		List param = new Vector();
		return JDAO.queryTemplateForColumnList(this.dbType, this.conn, this.queryRunner,  table, col, vm, suffixQuery, templateType, constraintType);
	}
	
	public List<Object>
	queryTemplateForColumnList(String table, String col, Map<String,String> vm, String suffixQuery)
			throws Exception
	{
		List param = new Vector();
		return JDAO.queryTemplateForColumnList(this.dbType, this.conn, this.queryRunner,  table, col, vm, suffixQuery);
	}
	
	public List<Object>
	queryTemplateForColumnList(String table, String col, Map<String,String> vm)
			throws Exception
	{
		List param = new Vector();
		return JDAO.queryTemplateForColumnList(this.dbType, this.conn, this.queryRunner,  table, col, vm);
	}
	
	public  Map<String,Object> queryForMap(String sql, Object... args)
			throws Exception
	{
		return JDAO.queryForMap(this.dbType, this.conn, this.queryRunner, sql, args);
	}
	
	public  Map<String,Object> queryForMap(String sql)
			throws Exception
	{
		return JDAO.queryForMap(this.dbType, this.conn, this.queryRunner, sql);
	}
	
	public  Map<String,Object> queryTemplateForMap(String table, Map<String,String> vm, List<String> fieldList, String suffixQuery, int templateType, int constraintType)
			throws Exception
	{
		return JDAO.queryTemplateForMap(this.dbType, this.conn, this.queryRunner,  table, fieldList, vm, suffixQuery, templateType, constraintType);
	}
	
	public  Map<String,Object> queryTemplateForMap(String table, Map<String,String> vm, List<String> fieldList, String suffixQuery)
			throws Exception
	{
		return JDAO.queryTemplateForMap(this.dbType, this.conn, this.queryRunner,  table, fieldList, vm, suffixQuery);
	}
	
	public  Map<String,Object> queryTemplateForMap(String table, Map<String,String> vm, List<String> fieldList)
			throws Exception
	{
		return JDAO.queryTemplateForMap(this.dbType, this.conn, this.queryRunner,  table, fieldList, vm);
	}
	
	public  Map<String,Object> queryTemplateForMap(String table, Map<String,String> vm)
			throws Exception
	{
		return JDAO.queryTemplateForMap(this.dbType, this.conn, this.queryRunner,  table, vm);
	}
	
	public  Map<String,String> queryForKvMap(String sql, Object... args)
			throws Exception
	{
		return JDAO.queryForKvMap(this.dbType, this.conn, this.queryRunner, sql, args);
	}
	
	public  Map<String,String> queryForKvMap(String sql)
			throws Exception
	{
		return JDAO.queryForKvMap(this.dbType, this.conn, this.queryRunner, sql);
	}
	
	public  Map<String,String>
	queryTemplateForKvMap(String table, String c1, String c2, Map<String,String> vm, String suffixQuery, int templateType, int constraintType)
			throws Exception
	{
		List param = new Vector();
		return JDAO.queryForKvMap(this.dbType, this.conn, this.queryRunner,  "SELECT "+c1+","+c2+" FROM "+table+" WHERE "+JDAO.buildWhere(dbType, templateType, constraintType, param, vm)+(suffixQuery==null?"":" "+suffixQuery), param);
	}
	
	public  Map<String,String>
	queryTemplateForKvMap(String table, String c1, String c2, Map<String,String> vm, String suffixQuery)
			throws Exception
	{
		return queryTemplateForKvMap(table, c1, c2, vm, suffixQuery, TEMPLATE_TYPE_AUTO, CONSTRAINT_ALL_OF);
	}
	
	public  Map<String,String>
	queryTemplateForKvMap(String table, String c1, String c2, Map<String,String> vm)
			throws Exception
	{
		return queryTemplateForKvMap(table, c1, c2, vm, null, TEMPLATE_TYPE_AUTO, CONSTRAINT_ALL_OF);
	}
	
	public  Map<String, List<String>> queryForKvListMap(String sql, Object... args)
			throws Exception
	{
		return JDAO.queryForKvListMap(this.dbType, this.conn, this.queryRunner, sql, args);
	}
	
	public  Map<String, List<String>> queryForKvListMap(String sql)
			throws Exception
	{
		return JDAO.queryForKvListMap(this.dbType, this.conn, this.queryRunner, sql);
	}
	
	public  Map<String, List<String>>
	queryTemplateForKvListMap(String table, String c1, String c2, Map<String,String> vm, String suffixQuery, int templateType, int constraintType)
			throws Exception
	{
		List param = new Vector();
		return JDAO.queryForKvListMap(this.dbType, this.conn, this.queryRunner,  "SELECT "+c1+","+c2+" FROM "+table+" WHERE "+JDAO.buildWhere(dbType, templateType, constraintType, param, vm)+(suffixQuery==null?"":" "+suffixQuery), param);
	}
	
	public  Map<String, List<String>>
	queryTemplateForKvListMap(String table, String c1, String c2, Map<String,String> vm, String suffixQuery)
			throws Exception
	{
		return queryTemplateForKvListMap(table, c1, c2, vm, suffixQuery, TEMPLATE_TYPE_AUTO, CONSTRAINT_ALL_OF);
	}
	
	public  Map<String, List<String>>
	queryTemplateForKvListMap(String table, String c1, String c2, Map<String,String> vm)
			throws Exception
	{
		return queryTemplateForKvListMap(table, c1, c2, vm, null, TEMPLATE_TYPE_AUTO, CONSTRAINT_ALL_OF);
	}
	
	public  <T> Map<String,T> queryForStringBeanMap(String sql, Class<T> beanClazz, Object... args)
			throws Exception
	{
		return this.queryForBeanMap(sql, String.class, beanClazz, args);
	}
	
	public  <T> Map<String,T> queryForStringBeanMap(String sql, Class<T> beanClazz)
			throws Exception
	{
		return this.queryForBeanMap(sql, String.class, beanClazz);
	}
	
	public  <K,V> Map<K,V> queryForBeanMap(String sql, Class<K> idClazz, Class<V> beanClazz, Object... args)
			throws Exception
	{
		return JDAO.queryForBeanMap(this.dbType, this.conn, this.queryRunner, sql, idClazz, beanClazz, args);
	}
	
	public  <K,V> Map<K,V> queryForBeanMap(String sql, Class<K> idClazz, Class<V> beanClazz)
			throws Exception
	{
		return JDAO.queryForBeanMap(this.dbType, this.conn, this.queryRunner, sql, idClazz, beanClazz);
	}
	
	public  <T> List<T> queryForBeanList(String sql, Class<T> beanClazz, Object... args)
			throws Exception
	{
		return JDAO.queryForBeanList(this.dbType, this.conn, this.queryRunner, sql, beanClazz, args);
	}
	
	public  <T> List<T> queryForBeanList(String sql, Class<T> beanClazz)
			throws Exception
	{
		return JDAO.queryForBeanList(this.dbType, this.conn, this.queryRunner, sql, beanClazz);
	}
	
	public  <T> T queryForBean(String sql, Class<T> beanClazz, Object... args)
			throws Exception
	{
		return JDAO.queryForBean(this.dbType, this.conn, this.queryRunner, sql, beanClazz, args);
	}
	
	public  <T> T queryForBean(String sql, Class<T> beanClazz)
			throws Exception
	{
		return JDAO.queryForBean(this.dbType, this.conn, this.queryRunner, sql, beanClazz);
	}
	
	public  Object queryForScalar(String sql, Object... args)
			throws Exception
	{
		return JDAO.queryForScalar(this.dbType, this.conn, this.queryRunner, sql, args);
	}
	
	public  Object queryForScalar(String sql)
			throws Exception
	{
		return JDAO.queryForScalar(this.dbType, this.conn, this.queryRunner, sql);
	}
	
	public  Object
	queryTemplateForScalar(String table, String col, Map<String,String> vm, String suffixQuery, int templateType, int constraintType)
			throws Exception
	{
		return JDAO.queryTemplateForScalar(this.dbType, this.conn, this.queryRunner,  table, col, vm, suffixQuery, templateType, constraintType);
	}
	
	public  Object
	queryTemplateForScalar(String table, String col, Map<String,String> vm, String suffixQuery)
			throws Exception
	{
		return JDAO.queryTemplateForScalar(this.dbType, this.conn, this.queryRunner,  table, col, vm, suffixQuery);
	}
	
	public  Object
	queryTemplateForScalar(String table, String col, Map<String,String> vm)
			throws Exception
	{
		return JDAO.queryTemplateForScalar(this.dbType, this.conn, this.queryRunner,  table, col, vm);
	}
	
	public  int update(String sql, Object... args)
			throws Exception
	{
		return JDAO.update(this.dbType, this.conn, this.queryRunner, sql, args);
	}
	
	public  int update(String sql)
			throws Exception
	{
		return JDAO.update(this.dbType, this.conn, this.queryRunner, sql);
	}
	
	public  int execute(String sql, Object... args)
			throws Exception
	{
		return JDAO.execute(this.dbType, this.conn, this.queryRunner, sql, args);
	}
	
	public  int execute(String sql)
			throws Exception
	{
		return JDAO.execute(this.dbType, this.conn, this.queryRunner, sql);
	}
	
	public  int insertSet(String table, Map cols)
			throws Exception
	{
		return JDAO.insertSet(this.dbType, this.conn, this.queryRunner, table, cols, false);
	}
	
	public  int insertSet(String table, Map cols, boolean onDuplicateKeyUpdate)
			throws Exception
	{
		return JDAO.insertSet(this.dbType, this.conn, this.queryRunner, table, cols, onDuplicateKeyUpdate);
	}
	
	public  int insertSet(String table, Map cols, boolean onDuplicateKeyUpdate, Collection updateCols)
			throws Exception
	{
		return JDAO.insertSet(this.dbType, this.conn, this.queryRunner, table, cols, onDuplicateKeyUpdate, updateCols);
	}
	
	public  int insert(String table, Map cols)
			throws Exception
	{
		return JDAO.insert(this.dbType, this.conn, this.queryRunner, table, cols);
	}
	
	public  int insert(String table, Map cols, boolean onDuplicateKeyUpdate)
			throws Exception
	{
		return JDAO.insert(this.dbType, this.conn, this.queryRunner, table, cols, onDuplicateKeyUpdate);
	}
	
	public  int insert(String table, Map cols, boolean onDuplicateKeyUpdate, Collection updateCols)
			throws Exception
	{
		return JDAO.insert(this.dbType, this.conn, this.queryRunner, table, cols, onDuplicateKeyUpdate, updateCols);
	}
	
	public void close()
	{
		try
		{
			if(conn!=null)
			{
				conn.close();
			}
		}
		catch(Exception xe) {}
		finally
		{
			conn=null;
			queryRunner=null;
		}
	}
	
}