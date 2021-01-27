package com.esni.recommendcommon.util;


import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class MysqlUtil {

    public static Connection getConnection() throws IOException, SQLException {

        Properties properties = PropertiesUtil.getProperties("database.properties");
        return DriverManager.getConnection(properties.getProperty("mysql.uri"),
                properties.getProperty("mysql.user"),
                properties.getProperty("mysql.password"));

    }

    public static Connection getConnection(Properties properties) throws SQLException {

        return DriverManager.getConnection(properties.getProperty("mysql.uri"),
                properties.getProperty("mysql.user"),
                properties.getProperty("mysql.password"));

    }

}

