package com.dyingbleed.akita;

import org.apache.commons.cli.*;
import org.apache.commons.io.IOUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by 李震 on 2017/12/4.
 */
public class Application {

    public static void main(String[] args) throws ParseException, IOException {
        Options options = new Options();

        options.addOption("p", true, "加载 Properties 文件");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        if (cmd.hasOption("p")) {
            String path = cmd.getOptionValue("p");

            // 加载 Properties
            Properties properties = loadProperties(path);

            // 初始化并启动 Akita
            Akita.createInstance(properties).run();
        } else {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp( "-p", options );
        }

    }


    private static Properties loadProperties(String path) throws IOException {
        InputStream in = new FileInputStream(path);
        Properties properties = new Properties();
        properties.load(in);
        IOUtils.closeQuietly(in);
        return properties;
    }

}
