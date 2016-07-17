import org.apache.commons.dbcp.BasicDataSource;
import org.apache.log4j.Logger;
import org.apache.spark.SparkFiles;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;

public class DataSourceFactory {
    private static Logger log = Logger.getLogger(DataSourceFactory.class);
    private static BasicDataSource bs = null;

    public static BasicDataSource getDataSource() throws Exception{
        if(bs==null){
            InputStream in = null;
            Properties prop = new Properties();

            try {
                in = new FileInputStream(new File(SparkFiles.get("dbcp.properties")));
                prop.load(in);
            }catch (Exception e){
                e.printStackTrace();
            }finally {
                in.close();
            }
            bs = new BasicDataSource();
            bs.setDriverClassName(prop.getProperty("driver"));
            bs.setUrl(prop.getProperty("mysqlurl"));
            bs.setUsername(prop.getProperty("uname"));
            bs.setPassword(prop.getProperty("pwd"));
            bs.setMaxActive(Integer.parseInt(prop.getProperty("maxActive")));//设置最大并发数
            bs.setInitialSize(Integer.parseInt(prop.getProperty("init")));//数据库初始化时，创建的连接个数
            bs.setMinIdle(Integer.parseInt(prop.getProperty("minIdle")));//最小空闲连接数
            bs.setMaxIdle(Integer.parseInt(prop.getProperty("maxIdle")));//数据库最大连接数
            bs.setMaxWait(1000);
            bs.setMinEvictableIdleTimeMillis(60*1000);//空闲连接60秒中后释放
            bs.setTimeBetweenEvictionRunsMillis(5*60*1000);//5分钟检测一次是否有死掉的线程
            bs.setTestOnBorrow(true);
        }
        return bs;
    }

    /**
     * 释放数据源
     */
    public static void shutDownDataSource() throws Exception{
        if(bs!=null){
            bs.close();
        }
    }

    public static Connection getConnection(){
        Connection con=null;
        try {
            if(bs!=null){
                con=bs.getConnection();
            }else{
                con=getDataSource().getConnection();
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return con;
    }

    /**
     * 关闭连接
     */
    public static void closeCon(ResultSet rs,PreparedStatement ps,Connection con){
        if(rs!=null){
            try {
                rs.close();
            } catch (Exception e) {
                log.error("关闭结果集ResultSet异常！"+e.getMessage(), e);
            }
        }
        if(ps!=null){
            try {
                ps.close();
            } catch (Exception e) {
                log.error("预编译SQL语句对象PreparedStatement关闭异常！"+e.getMessage(), e);
            }
        }
        if(con!=null){
            try {
                con.close();
            } catch (Exception e) {
                log.error("关闭连接对象Connection异常！"+e.getMessage(), e);
            }
        }
    }
}
