package org.apache.rocketmq.mysql.connector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Test {

    public static void main(String[] args) {
        //声明Connection对象
        Connection con;
        //驱动程序名
        String driver = "com.mysql.jdbc.Driver";
        //URL指向要访问的数据库名mydata
        String url = "jdbc:mysql://localhost:3306/db_name";
        //MySQL配置时的用户名
        String user = "debezium";
        //MySQL配置时的密码
        String password = "debezium";
        //遍历查询结果集
        try {
            //加载驱动程序
            Class.forName(driver);
            //1.getConnection()方法，连接MySQL数据库！！
            con = DriverManager.getConnection(url,user,password);
            if(!con.isClosed())
                System.out.println("Succeeded connecting to the Database!");
            String name;
            String id;

            PreparedStatement psql;

            for(int j=0;j<100;j++){
                Thread.sleep(1);
                for(int i=0;i<1000;i++){
                    psql = con.prepareStatement("insert into course (gmt_create,gmt_modified,course_name,course_no) "
                        + "values(?,?,?,?)");

                    DateFormat dateFormat2 = new SimpleDateFormat("yyyy-MM-dd");
                    Date myDate2 = dateFormat2.parse("2010-09-13");
                    psql.setDate(1,new java.sql.Date(myDate2.getTime()));
                    psql.setDate(2,new java.sql.Date(myDate2.getTime()));
                    psql.setString(3, "course_name"+i);      //设置参数2，name 为王刚
                    psql.setInt(4, 1);
                    psql.executeUpdate();           //执行更新
                }
            }

            con.close();
        } catch(ClassNotFoundException e) {
            //数据库驱动类异常处理
            System.out.println("Sorry,can`t find the Driver!");
            e.printStackTrace();
        } catch(SQLException e) {
            //数据库连接失败异常处理
            e.printStackTrace();
        }catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
        }finally{
            System.out.println("数据库数据成功获取！！");
        }
    }

}