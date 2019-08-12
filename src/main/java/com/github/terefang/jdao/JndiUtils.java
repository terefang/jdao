package com.github.terefang.jdao;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.naming.*;
import javax.sql.DataSource;
import java.sql.Connection;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;

public class JndiUtils
{
    public static Log LOG = LogFactory.getLog(JndiUtils.class);

    public static void log(String text)
    {
        LOG.info(text);
    }

    public static void log(String text, Throwable thx)
    {
        LOG.info(text, thx);
    }


    public static final String DEFAULT_JNDI_PATH = "java:ds";

    public static Map<String, DataSource> registerJndiDsFromDirectories(List<String> configDirs, String filesuffix, Context env)
    {
        if(filesuffix==null)
        {
            filesuffix = JdaoUtils.PROP_DATASOURCE_CONFIG_SUFFIX;
        }

        Map<String, DataSource> dsources = JdaoUtils.scanRecursiveDatasourceDirectories(configDirs, filesuffix);

        registerJNDI(env, dsources);

        return dsources;
    }

    public static void registerJNDI(Context env, Map<String, DataSource> dsources)
    {
        for(Map.Entry<String,DataSource> entry : dsources.entrySet())
        {
            String dsName = entry.getKey();
            try
            {
                DataSource dataSource = entry.getValue();

                bind(env, dsName, dataSource);

                log("registered ds='"+dsName+"'");
            }
            catch(Exception xe)
            {
                log("Error processing datasource: "+dsName, xe);
            }
        }
    }

    public static void unregisterJNDI(Context env, Map<String, DataSource> dsources)
    {
        for(Map.Entry<String,DataSource> entry : dsources.entrySet())
        {
            String dsName = entry.getKey();
            try
            {

                unbind(env, dsName);

                log("unregistered ds='"+dsName+"'");
            }
            catch(Exception xe)
            {
                log("Error unregister datasource: "+dsName, xe);
            }
        }
    }

    public static Context retrieveContext(String jndi_path)
    {
        InitialContext jndiContext = null;
        Context env = null;
        try
        {
            log("INFO: resolving "+jndi_path);

            env = jndiContext = new InitialContext();
            env = (Context)jndiContext.lookup(jndi_path);
        }
        catch(Exception xe)
        {
            try
            {
                Name jname = jndiContext.getNameParser(jndi_path).parse(jndi_path);
                Enumeration<String> en = jname.getAll();
                while(en.hasMoreElements())
                {
                    String name = en.nextElement();
                    Context tmp = null;
                    try
                    {
                        tmp = (Context)env.lookup(name);
                        env=(Context)env.lookup(name);
                    }
                    catch(NameNotFoundException nnf)
                    {
                        log("INFO: creating "+name);
                        env = env.createSubcontext(name);
                    }
                }
            }
            catch(Exception xe2)
            {
                log("ERROR: resolving "+jndi_path, xe2);
            }
        }
        return env;
    }

    public static Context unbind (Context ctx, String nameStr)
            throws NamingException
    {
        log("unbinding "+nameStr);

        Name name = ctx.getNameParser("").parse(nameStr);

        //no name, nothing to do
        if (name.size() == 0)
            return null;

        Context subCtx = ctx;

        for (int i=0; i < name.size() - 1; i++)
        {
            try
            {
                subCtx = (Context)subCtx.lookup (name.get(i));
            }
            catch (NameNotFoundException e)
            {
                log("Subcontext "+name.get(i)+" undefined", e);
                return null;
            }
        }

        subCtx.unbind(name.get(name.size() - 1));
        log("unbound object "+nameStr);
        return subCtx;
    }

    public static Context bind (Context ctx, String nameStr, Object obj)
            throws NamingException
    {
        log("binding "+nameStr);

        Name name = ctx.getNameParser("").parse(nameStr);

        //no name, nothing to do
        if (name.size() == 0)
            return null;

        Context subCtx = ctx;

        //last component of the name will be the name to bind
        for (int i=0; i < name.size() - 1; i++)
        {
            try
            {
                subCtx = (Context)subCtx.lookup (name.get(i));
                log("Subcontext "+name.get(i)+" already exists");
            }
            catch (NameNotFoundException e)
            {
                subCtx = subCtx.createSubcontext(name.get(i));
                log("Subcontext "+name.get(i)+" created");
            }
        }

        subCtx.rebind (name.get(name.size() - 1), obj);
        log("Bound object to "+name.get(name.size() - 1));
        return subCtx;
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


}
