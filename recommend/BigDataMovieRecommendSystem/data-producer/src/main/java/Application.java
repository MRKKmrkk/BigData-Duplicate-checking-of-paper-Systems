import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Random;

public class Application {


    public static void main(String[] args) throws IOException {

//        ClassLoader loader = Thread.currentThread().getContextClassLoader();
//        InputStream in = loader.getResourceAsStream("data-producer.properties");
//        Properties properties = new Properties();
//        properties.load(in);
//
//        System.out.println(properties.getProperty("userid.max.index") + 1);
//        Logger logger = LoggerFactory.getLogger("data-producer");
//
//        logger.info("test");

        Random random = new Random();
        for (int i = 0; i < 10; i++) {
            System.out.println();
        }

    }

}
