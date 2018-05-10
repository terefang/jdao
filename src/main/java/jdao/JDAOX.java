package jdao;

import org.apache.commons.dbutils.*;
import org.apache.commons.dbutils.handlers.BeanHandler;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import org.apache.commons.dbutils.handlers.BeanMapHandler;
import org.apache.commons.lang3.reflect.FieldUtils;

import java.beans.PropertyDescriptor;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.lang.reflect.Field;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import java.lang.annotation.ElementType;
import java.lang.annotation.RetentionPolicy;

public class JDAOX
{
	public interface IBean
	{
	}
	
	@Target(value=ElementType.FIELD)
	@Retention(value=RetentionPolicy.RUNTIME)
	public @interface IBeanField
	{
		String value();
	}

	JDAO dao;
	
	public static synchronized final JDAOX wrap(JDAO d)
	{
		JDAOX jdaox = new JDAOX();
		jdaox.dao = d;
		return jdaox;
	}
	
	public void close()
	{
		dao.close();
	}
	
	
	public <T> Map<String, T> queryForStringBeanMap(String sql, Class<T> beanClazz, Object... args)
			throws Exception
	{
		return queryForBeanMap(sql, String.class, beanClazz, args);
	}
	
	public <T> Map<String, T> queryForStringBeanMap(String sql, Class<T> beanClazz)
			throws Exception
	{
		return queryForBeanMap(sql, String.class, beanClazz);
	}
	
	public <K, V> Map<K, V> queryForBeanMap(String sql, Class<K> idClazz, Class<V> beanClazz, Object... args)
			throws Exception
	{
		BeanMapHandler<K, V> handler = new BeanMapHandler<K, V>(beanClazz, IBean.class.isAssignableFrom(beanClazz)
				? new BasicRowProcessor(IBeanProcessor.of(beanClazz)) : this.dao.generousRowProcessor);
		
		return this.dao.queryForT(this.dao.getDbType(), handler, this.dao.getConnection(), this.dao.getQueryRunner(), sql, args);
	}
	
	public <K, V> Map<K, V> queryForBeanMap(String sql, Class<K> idClazz, Class<V> beanClazz)
			throws Exception
	{
		BeanMapHandler<K, V> handler = new BeanMapHandler<K, V>(beanClazz, IBean.class.isAssignableFrom(beanClazz)
				? new BasicRowProcessor(IBeanProcessor.of(beanClazz)) : this.dao.generousRowProcessor);
		
		return this.dao.queryForT(this.dao.getDbType(), handler, this.dao.getConnection(), this.dao.getQueryRunner(), sql);
	}
	
	public <T> List<T> queryForBeanList(String sql, Class<T> beanClazz, Object... args)
			throws Exception
	{
		ResultSetHandler<List<T>> handler = new BeanListHandler<T>(beanClazz, IBean.class.isAssignableFrom(beanClazz)
				? new BasicRowProcessor(IBeanProcessor.of(beanClazz)) : this.dao.generousRowProcessor);
		
		return this.dao.queryForT(this.dao.getDbType(), handler, this.dao.getConnection(), this.dao.getQueryRunner(), sql, args);
	}
	
	public <T> List<T> queryForBeanList(String sql, Class<T> beanClazz)
			throws Exception
	{
		ResultSetHandler<List<T>> handler = new BeanListHandler<T>(beanClazz, IBean.class.isAssignableFrom(beanClazz)
				? new BasicRowProcessor(IBeanProcessor.of(beanClazz)) : this.dao.generousRowProcessor);
		
		return this.dao.queryForT(this.dao.getDbType(), handler, this.dao.getConnection(), this.dao.getQueryRunner(), sql);
	}
	
	public <T> T queryForBean(String sql, Class<T> beanClazz, Object... args)
			throws Exception
	{
		ResultSetHandler<T> handler = new BeanHandler<T>(beanClazz, IBean.class.isAssignableFrom(beanClazz)
				? new BasicRowProcessor(IBeanProcessor.of(beanClazz)) : this.dao.generousRowProcessor);
		
		return this.dao.queryForT(this.dao.getDbType(), handler, this.dao.getConnection(), this.dao.getQueryRunner(), sql, args);
	}
	
	
	public <T> T queryForBean(String sql, Class<T> beanClazz)
			throws Exception
	{
		ResultSetHandler<T> handler = new BeanHandler<T>(beanClazz, IBean.class.isAssignableFrom(beanClazz)
				? new BasicRowProcessor(IBeanProcessor.of(beanClazz)) : this.dao.generousRowProcessor);
		
		return this.dao.queryForT(this.dao.getDbType(), handler, this.dao.getConnection(), this.dao.getQueryRunner(), sql);
	}
	
	public static class IBeanProcessor<T> extends BeanProcessor
	{
		private Class<T> type;
		
		public static <T> IBeanProcessor<T> of(Class<T> type)
		{
			IBeanProcessor abp = new IBeanProcessor();
			abp.type = type;
			return abp;
		}
		
		private IBeanProcessor()
		{
		}
		
		protected int[] mapColumnsToProperties(ResultSetMetaData rsmd, PropertyDescriptor[] props) throws SQLException
		{
			int cols = rsmd.getColumnCount();
			int[] columnToProperty = new int[ cols+1];
			
			Arrays.fill(columnToProperty, -1);
			
			List<Field> fieldList = FieldUtils.getFieldsListWithAnnotation(this.type, IBeanField.class);
			for(int col = 1; col <= cols; ++col)
			{
				String columnName = rsmd.getColumnLabel(col);
				if(null == columnName || 0 == columnName.length())
				{
					columnName = rsmd.getColumnName(col);
				}
				
				for(Field f : fieldList)
				{
					String fName = f.getAnnotation(IBeanField.class).value();
					String pName = f.getName();
					if(fName.equalsIgnoreCase(columnName))
					{
						for(int i = 0; i < props.length; ++i)
						{
							String propName = props[i].getName();
							if(pName.equalsIgnoreCase(propName))
							{
								columnToProperty[col] = i;
								break;
							}
						}
						break;
					}
				}
			}
			
			return columnToProperty;
		}
	}
}
