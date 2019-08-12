package com.github.terefang.jdao;

import org.apache.commons.dbutils.BasicRowProcessor;
import org.apache.commons.dbutils.BeanProcessor;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.handlers.BeanHandler;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import org.apache.commons.dbutils.handlers.BeanMapHandler;
import org.apache.commons.lang3.reflect.FieldUtils;

import java.beans.PropertyDescriptor;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Field;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;

public class JBAO 
{
    public static JBAO from(JDAO _dao) 
    {
        JBAO _daox = new JBAO();
        _daox._dao = _dao;
        return  _daox;
    }

    JDAO _dao = null;
    
    public interface IBean
    {
    }

    @Target(value= ElementType.FIELD)
    @Retention(value= RetentionPolicy.RUNTIME)
    public @interface IBeanField
    {
        String value();
    }

    @Target(value= ElementType.FIELD)
    @Retention(value= RetentionPolicy.RUNTIME)
    public @interface IBeanID
    {
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
                ? new BasicRowProcessor(IBeanProcessor.of(beanClazz)) : this._dao.generousRowProcessor);

        return this._dao.queryForT(this._dao.getDbType(), handler, this._dao.getConnection(), this._dao.getQueryRunner(), sql, args);
    }

    public <K, V> Map<K, V> queryForBeanMap(String sql, Class<K> idClazz, Class<V> beanClazz)
            throws Exception
    {
        BeanMapHandler<K, V> handler = new BeanMapHandler<K, V>(beanClazz, IBean.class.isAssignableFrom(beanClazz)
                ? new BasicRowProcessor(IBeanProcessor.of(beanClazz)) : this._dao.generousRowProcessor);

        return this._dao.queryForT(this._dao.getDbType(), handler, this._dao.getConnection(), this._dao.getQueryRunner(), sql);
    }

    public <T> List<T> queryForBeanList(String sql, Class<T> beanClazz, Object... args)
            throws Exception
    {
        ResultSetHandler<List<T>> handler = new BeanListHandler<T>(beanClazz, IBean.class.isAssignableFrom(beanClazz)
                ? new BasicRowProcessor(IBeanProcessor.of(beanClazz)) : this._dao.generousRowProcessor);

        return this._dao.queryForT(this._dao.getDbType(), handler, this._dao.getConnection(), this._dao.getQueryRunner(), sql, args);
    }

    public <T> List<T> queryForBeanList(String sql, Class<T> beanClazz)
            throws Exception
    {
        ResultSetHandler<List<T>> handler = new BeanListHandler<T>(beanClazz, IBean.class.isAssignableFrom(beanClazz)
                ? new BasicRowProcessor(IBeanProcessor.of(beanClazz)) : this._dao.generousRowProcessor);

        return this._dao.queryForT(this._dao.getDbType(), handler, this._dao.getConnection(), this._dao.getQueryRunner(), sql);
    }

    public <T> T queryForBean(String sql, Class<T> beanClazz, Object... args)
            throws Exception
    {
        ResultSetHandler<T> handler = new BeanHandler<T>(beanClazz, IBean.class.isAssignableFrom(beanClazz)
                ? new BasicRowProcessor(IBeanProcessor.of(beanClazz)) : this._dao.generousRowProcessor);

        return this._dao.queryForT(this._dao.getDbType(), handler, this._dao.getConnection(), this._dao.getQueryRunner(), sql, args);
    }


    public <T> T queryForBean(String sql, Class<T> beanClazz)
            throws Exception
    {
        ResultSetHandler<T> handler = new BeanHandler<T>(beanClazz, IBean.class.isAssignableFrom(beanClazz)
                ? new BasicRowProcessor(IBeanProcessor.of(beanClazz)) : this._dao.generousRowProcessor);

        return this._dao.queryForT(this._dao.getDbType(), handler, this._dao.getConnection(), this._dao.getQueryRunner(), sql);
    }

    public static <T>String extractIdNameFromBean(Class<T> _beanClazz)
    {
        List<Field> fields = FieldUtils.getFieldsListWithAnnotation(_beanClazz, IBeanID.class);
        if(fields.size()!=1)
        {
            throw new IllegalArgumentException("improper IBeanID annotation");
        }
        return fields.iterator().next().getAnnotation(IBeanField.class).value();
    }

    public static <T>Map<String,Object> extractIdKvFromBean(Object _bean, Class<T> _beanClazz)
    {
        Map<String,Object> _ret = new HashMap();
        List<Field> fields = FieldUtils.getFieldsListWithAnnotation(_beanClazz, IBeanID.class);
        if(fields.size()>0)
        {
            throw new IllegalArgumentException("improper IBeanID annotation");
        }
        try
        {
            Iterator<Field> it = fields.iterator();
            while(it.hasNext())
            {
                Field field = it.next();
                String _key = field.getAnnotation(IBeanField.class).value();
                Object _val = FieldUtils.readField(field, _bean, true);
                _ret.put(_key, _val);
            }
            return _ret;
        }
        catch(IllegalAccessException e)
        {
            // IGNORE
        }
        return null;
    }

    public static <T>Map<String,Object> extractColsFromBean(Object _bean, Class<T> _beanClazz, boolean _inclIdField)
    {
        Map<String,Object> _ret = new HashMap();
        List<Field> fields = FieldUtils.getFieldsListWithAnnotation(_beanClazz, IBeanField.class);
        for(Field field : fields)
        {
            try
            {
                if(_inclIdField || (!field.isAnnotationPresent(IBeanID.class)))
                {
                    String _key = field.getAnnotation(IBeanField.class).value();
                    Object _val = FieldUtils.readField(field, _bean, true);
                    _ret.put(_key, _val);
                }
            }
            catch(IllegalAccessException e)
            {
                // IGNORE
            }
        }
        return _ret;
    }

    public static <T> List<Map<String,Object>> extractColsFromBeanList(List<Object> _beans, Class<T> _beanClazz, boolean _inclIdField)
    {
        List<Map<String,Object>> _ret = new Vector();
        for(Object b : _beans)
        {
            _ret.add(extractColsFromBean((T)b, _beanClazz, _inclIdField));
        }
        return _ret;
    }

    public <T> int insertBean(String _table, IBean bean, Class<T> beanClazz)
            throws Exception
    {
        Map<String,Object> _col = extractColsFromBean((T)bean, beanClazz, true);
        return this._dao.insert(_table, _col);
    }

    public <T> int insertBeanBySQL(String _query, IBean bean, Class<T> beanClazz)
            throws Exception
    {
        Map<String,Object> _col = extractColsFromBean((T)bean, beanClazz, true);
        return _dao.execute(this._dao.getDbType(), this._dao.getConnection(), this._dao.getQueryRunner(), _query, _col);
    }

    public <T>int updateBean(String table, Object _bean, Class<T> _beanClazz)
            throws Exception
    {
        Map<String,Object> keyCol = extractIdKvFromBean(_bean, _beanClazz);
        Map cols = extractColsFromBean(_bean, _beanClazz, false);

        Vector vv = new Vector();
        String setqq = _dao.buildSet(this._dao.getDbType(), vv, cols);
        String wqq = _dao.buildWhereEqual(this._dao.getDbType(), vv, keyCol);

        StringBuilder qq=new StringBuilder();

        qq.append("UPDATE "+table+" SET ");
        qq.append(setqq);
        qq.append(" WHERE ");
        qq.append(wqq);

        return this._dao.update(qq.toString(), vv.toArray());
    }

    public <T>int updateBeans(String table, List<Object> _beans, Class<T> _beanClazz)
            throws Exception
    {
        int _ret = 0;
        for(Object _bean : _beans)
        {
            _ret += updateBean(table, _bean, _beanClazz);
        }
        return _ret;
    }

    public void checkReadOnly() throws Exception {
        _dao.checkReadOnly();
    }

    public boolean isReadOnly() {
        return _dao.isReadOnly();
    }

    public void setAutoCommit(boolean ac) {
        _dao.setAutoCommit(ac);
    }

    public void beginTransaction() {
        _dao.beginTransaction();
    }

    public void rollbackTransaction() {
        _dao.rollbackTransaction();
    }

    public void commitTransaction() {
        _dao.commitTransaction();
    }

    public void close() {
        _dao.close();
    }
}
