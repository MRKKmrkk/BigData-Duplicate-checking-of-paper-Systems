package com.esni.recommendcommon.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertiesUtil {

    public static Properties getProperties(String resourceName) throws IOException {

        InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(resourceName);
        Properties properties = new Properties();
        properties.load(in);

        return properties;

    }

}
