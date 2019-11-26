import java.sql.*;

public class MySQLDemo {

  static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
  static final String DB_URL = "jdbc:mysql://localhost:3306/RUNOOB";

  static final String USER = "root";
  static final String PASS = "Peng_cheng_13";

  static Statement stmt = null;

  public static int open(String path) throws SQLException {
    //String sqlcommand = "Select * from " + path;
    String sqlcommand = "show tables like '" + path + "'";
    System.out.print(sqlcommand + "\n");
    ResultSet rs2 = null;
    if (stmt != null)
      rs2 = exe_command(stmt, sqlcommand);
    else
      System.out.print("Env error!\n");
    int j = 0;
    while(rs2.next())
      j++;
    if (j == 0)
      return -1;
    else
      return 0;
  }

  private static ResultSet exe_command(Statement stmt, String sql) throws SQLException {
    ResultSet rs1 = stmt.executeQuery(sql);
    return rs1;
  }


  public static void main(String[] args) {
    Connection conn = null;
    //Statement stmt = null;

    try {
      //Conect
      Class.forName("com.mysql.jdbc.Driver");
      System.out.println("连接数据库...");
      conn = DriverManager.getConnection(DB_URL,USER,PASS);

      stmt = conn.createStatement();
      String sql;
      sql = "SELECT id, name, url FROM website";
      int myopen = open("website");
      if (myopen == 0)
         System.out.print("Table exists\n");
      else
        System.out.print("Table is not exists\n");
      //sql = args[0];
      ResultSet rs = stmt.executeQuery(sql);

      while (rs.next()) {
        int id  = rs.getInt("id");
        String name = rs.getString("name");
        String url = rs.getString("url");
        System.out.print("ID: " + id);
        System.out.print(", 站点名称: " + name);
        System.out.print(", 站点 URL: " + url);
        System.out.print("\n");
      }
      rs.close();
      stmt.close();
      conn.close();
    } catch (SQLException se) {
      se.printStackTrace();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      try {
        if(stmt!=null) stmt.close();
      } catch (SQLException se2) {
      }
      try {
        if(conn!=null) conn.close();
      } catch (SQLException se) {
        se.printStackTrace();
      }
    }
    System.out.println("Goodbye!");
  }

}
