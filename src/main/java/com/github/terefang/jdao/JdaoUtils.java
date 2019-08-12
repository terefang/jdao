package com.github.terefang.jdao;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.dbcp.BasicDataSourceFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.sql.DataSource;
import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class JdaoUtils {
    public static Log LOG = LogFactory.getLog(JndiUtils.class);

    public static void log(String text)
    {
        LOG.info(text);
    }

    public static void log(String text, Throwable thx)
    {
        LOG.info(text, thx);
    }

    public static final String PROP_DATASOURCE_CONFIG_SUFFIX = ".ds.properties";

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

    public static void adjustPropertiesForEnvParameters(Properties properties)
    {
        for(String key : properties.stringPropertyNames())
        {
            String property = properties.getProperty(key);
            int ofs = 0;
            while((ofs = property.indexOf("$(", ofs)) > 0)
            {
                int efs = property.indexOf(')', ofs);
                String lookupKey = property.substring(ofs+2, efs);
                String lookupProp = System.getProperty(lookupKey,"$("+lookupKey+")");
                property = property.substring(0,ofs)+lookupProp+property.substring(efs+1);
                ofs++;
                properties.setProperty(key, property);
            }
        }
    }

    public static DataSource createDataSourceByProperties(String file, DataSource dataSource, Properties properties)
    {
        try
        {
            adjustPropertiesForEnvParameters(properties);
            if(dataSource==null)
            {
                if(properties.containsKey("jdaoDriverClassName"))
                {
                    Class.forName(properties.getProperty("jdaoDriverClassName"));
                }
                if(properties.containsKey("jdaoDataSourceClassName"))
                {
                    dataSource = (DataSource)Thread.currentThread().getContextClassLoader().loadClass(properties.getProperty("jdaoDataSourceClassName")).newInstance();
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

    public static DataSource createDataSourceByProperties(Class clazz, Properties properties)
    {
        try
        {
            adjustPropertiesForEnvParameters(properties);
            DataSource dataSource = null;
            dataSource = (DataSource)clazz.newInstance();
            BeanUtils.populate(dataSource, (Map)properties);
            return dataSource;
        }
        catch(Exception xe)
        {
            log("Error processing datasource class: "+clazz.getCanonicalName(), xe);
            return null;
        }
    }

    public static DataSource createDataSourceByProperties(String clazz, Properties properties)
    {
        try
        {
            return createDataSourceByProperties(Class.forName(clazz, true, Thread.currentThread().getContextClassLoader()), properties);
        }
        catch(Exception xe)
        {
            log("Error processing datasource class: "+clazz, xe);
            return null;
        }
    }

    public static Connection createConnectionByDriverSpec(String driverclazz, String jdbcUri, String userName, String password)
    {
        try
        {
            if(driverclazz!=null && !driverclazz.equalsIgnoreCase(""))
            {
                Class.forName(driverclazz, true, Thread.currentThread().getContextClassLoader());
            }
            return DriverManager.getConnection(jdbcUri, userName, password);
        }
        catch(Exception xe)
        {
            log("Error : ", xe);
            return null;
        }
    }

    public static Connection createConnectionByDataSourceSpec(String dsclazz, String jdbcUri, String userName, String password)
    {
        try
        {
            if(dsclazz!=null && !dsclazz.equalsIgnoreCase(""))
            {
                DataSource ds = (DataSource)Class.forName(dsclazz, true, Thread.currentThread().getContextClassLoader()).newInstance();
                BeanUtils.setProperty(ds, "url", jdbcUri);
                return ds.getConnection(userName, password);
            }
            return DriverManager.getConnection(jdbcUri, userName, password);
        }
        catch(Exception xe)
        {
            log("Error : ", xe);
            return null;
        }
    }

}
