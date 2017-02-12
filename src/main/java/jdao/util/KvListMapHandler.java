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
import org.apache.commons.dbutils.ResultSetHandler;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;


public class KvListMapHandler implements ResultSetHandler<Map<String,List<String>>>
{
	BasicRowProcessor basicRowProcessor = new BasicRowProcessor();

	public KvListMapHandler() { super(); }

	public Map<String, List<String>> handle(ResultSet rs) 
	throws SQLException 
	{
		Map<String, List<String>> ret = new LinkedHashMap<String, List<String>>();
		while(rs.next())
		{
			Object[] row = basicRowProcessor.toArray(rs);
			if(row.length>=2)
			{
				String key = String.valueOf(row[0]);
				if(!ret.containsKey(key))
				{
					ret.put(key, new Vector<String>());
				}
				ret.get(key).add(String.valueOf(row[1]));
			}
		}
		return ret;
	}
}