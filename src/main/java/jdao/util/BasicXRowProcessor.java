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
package jdao.util;

import org.apache.commons.dbutils.BasicRowProcessor;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


public class BasicXRowProcessor extends BasicRowProcessor
{

	public BasicXRowProcessor() { super(); }
	 
	@Override
	public Map<String, Object> toMap(ResultSet rs) throws SQLException 
	{
		Map result = new CaseInsensitiveHashMap();
		ResultSetMetaData rsmd = rs.getMetaData();
		int cols = rsmd.getColumnCount();
		
		for (int i = 1; i <= cols; i++) {
			if(rsmd.getColumnLabel(i)!=null)
			{
				result.put(rsmd.getColumnLabel(i), rs.getObject(i));
			}
			else
			{
				result.put(rsmd.getColumnName(i), rs.getObject(i));
			}
		}
		
		return result;
	}

	private static class CaseInsensitiveHashMap extends HashMap 
	{
        /**
         * @see Map#containsKey(Object)
         */
        public boolean containsKey(Object key) 
        {
            return super.containsKey(key.toString().toLowerCase());
        }

        /**
         * @see Map#get(Object)
         */
        public Object get(Object key) 
        {
            return super.get(key.toString().toLowerCase());
        }

        /**
         * @see Map#put(Object, Object)
         */
        public Object put(Object key, Object value) 
        {
            return super.put(key.toString().toLowerCase(), value);
        }

        /**
         * @see Map#putAll(Map)
         */
        public void putAll(Map m) 
        {
            Iterator iter = m.keySet().iterator();
            while (iter.hasNext()) 
            {
                Object key = iter.next();
                Object value = m.get(key);
                this.put(key, value);
            }
        }

        /**
         * @see Map#remove(Object)
         */
        public Object remove(Object key) 
        {
            return super.remove(key.toString().toLowerCase());
        }
    }
}
