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

package jdao.sevlet;

import jdao.JDAO;

import javax.naming.Context;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import javax.sql.DataSource;

import java.util.Collections;
import java.util.Map;

public class JdaoJndiServletListener implements ServletContextListener
{
	public static final String PROP_CONFIG_PROPERTY = "jdao.jndi.config.path";
	
	public static final String PROP_CONFIG_DEFAULT = "WEB-INF/datasources";
	
	public static final String PROP_JNDI_PATH_PROPERTY = "jdao.jndi.reg.path";
	
	private ServletContext servletContext;
	
	private Context daoContext ;
	
	private Map<String, DataSource> dataSources;
		
	@Override
	public void contextInitialized(ServletContextEvent servletContextEvent)
	{
		this.servletContext = servletContextEvent.getServletContext();
		this.servletContext.log("init JdaoJndiServletListener");
		
		String configDirs = servletContext.getInitParameter(PROP_CONFIG_PROPERTY);
		
		if(configDirs == null)
		{
			this.servletContext.log("WARN: no context property '"+PROP_CONFIG_PROPERTY+"' found.");
			
			configDirs = PROP_CONFIG_DEFAULT;
		}
		
		String jndi_path = servletContext.getInitParameter(PROP_JNDI_PATH_PROPERTY);
		
		if(jndi_path == null)
		{
			this.servletContext.log("WARN: no context property '"+PROP_JNDI_PATH_PROPERTY+"' found.");
			
			jndi_path = JDAO.DEFAULT_JNDI_PATH;
		}
		
		this.daoContext = JDAO.retrieveContext(jndi_path);
		
		this.dataSources = JDAO.registerJndiDsFromDirectories(Collections.singletonList(configDirs), JDAO.PROP_DATASOURCE_CONFIG_SUFFIX, this.daoContext);
	}
	
	@Override
	public void contextDestroyed(ServletContextEvent servletContextEvent)
	{
		this.servletContext.log("destroy JdaoJndiServletListener");
		
		JDAO.unregisterJNDI(this.daoContext, this.dataSources);
		
		this.dataSources.clear();

		try { this.daoContext.close(); } catch(Exception xe) {}
	}
}
