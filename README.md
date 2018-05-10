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

