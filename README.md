## JDAO

Java Database Access Object -- not another ORM layer

* tries to combine the simplicity of PHP's PDO
* with the richness of Spring's JDBCTemplate
* thin (not tiny) layer over Apache's DBUtils and DBCP
* can build common query code based on templating with simple maps and lists
* intended to be used in Java as-well-as JVM Scripting Languages

## Example

    Connection conn = JDAO.createConnectionByDriverSpec("com.mysql.jdbc.Driver", "jdbc:mysql://127.0.0.1:3307/test", "sa", "sa");
    JDAO dao = JDAO.createDaoFromConnection(conn, false);
    List rows = dao.queryForList("SELECT * FROM Test_User_Table WHERE UserName = ?", "stiger");

## JDAOX

Experimental JDAO Features Wrapper Class

* staging ground for experimental/ustable features
* if stable features will be migrated to JDAO

### New IBean Handling

    public static final void main(String[] args) throws Exception
	{
		Connection conn = JDAO.createConnectionByDriverSpec("org.sqlite.JDBC", "jdbc:sqlite::memory:", "sa", "sa");
		JDAO jdao = JDAO.createDaoFromConnection(conn, true);
		JDAOX jdaox = JDAOX.wrap(jdao);
		
		sql_row_bean bean = jdaox.queryForBean("SELECT 'this is name' AS prefix_name, 'this is value' AS prefix_value ", sql_row_bean.class);
		
		jdaox.close();
		
		System.out.println("name="+bean.name);
		System.out.println("value="+bean.value);
	}
	
	public static class sql_row_bean implements JDAOX.IBean
	{
		@JDAOX.IBeanField("prefix_name")
		public String name;
		
		@JDAOX.IBeanField("prefix_value")
		public String value;
		
		public String getName()
		{
			return name;
		}
		
		public void setName(String name)
		{
			this.name = name;
		}
		
		public String getValue()
		{
			return value;
		}
		
		public void setValue(String value)
		{
			this.value = value;
		}
	}
