package de.tu_berlin.dos.demeter.optimizer.utils;

import de.tu_berlin.dos.demeter.optimizer.structures.OrderedProperties;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.lang.reflect.Method;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.stream.Stream;

public enum FileManager { GET;

    public static final Logger LOG = LogManager.getLogger(FileManager.class);

    private static final Map<String, Object> fileMap = new HashMap<>();

    public String path(String fileName) throws Exception {

        URL resource = FileManager.GET.getClass().getClassLoader().getResource(fileName);
        return resource.getFile();
    }

    public File tmpWithContents(String contents) throws Exception {

        final File tempFile = File.createTempFile("stream2file", ".tmp");
        tempFile.deleteOnExit();
        try (FileOutputStream output = new FileOutputStream(tempFile)) {
            IOUtils.write(contents, output, Charset.defaultCharset());
        }
        return tempFile;
    }

    public <T> T resource(String fileName, Class<T> clazz) throws Exception {

        if (!fileMap.containsKey(fileName)) {

            try (InputStream input = FileManager.GET.getClass().getClassLoader().getResourceAsStream(fileName)) {
            
                if (clazz == Properties.class) {

                    Object o = OrderedProperties.class.getDeclaredConstructor().newInstance();
                    Method method = clazz.getMethod("load", InputStream.class);
                    method.invoke(o, input);
                    fileMap.put(fileName, o);
                }
                if (clazz == OrderedProperties.class) {
    
                    Object o = OrderedProperties.class.getDeclaredConstructor().newInstance();
                    Method method = clazz.getMethod("load", InputStream.class);
                    method.invoke(o, input);
                    fileMap.put(fileName, o);
                }
                else if (clazz == File.class) {
    
                    final File tempFile = File.createTempFile("stream2file", ".tmp");
                    tempFile.deleteOnExit();
                    try (FileOutputStream output = new FileOutputStream(tempFile)) {

                        IOUtils.copy(input, output);
                    }
                    fileMap.put(fileName, tempFile);
                }
                else {
                    throw new IllegalStateException("Unrecognized file type: " + clazz);
                }
            }
        }
        return clazz.cast(fileMap.get(fileName));
    }

    public void replace(String fileName, String regex, String replacement) throws Exception {

        Path path = Paths.get(fileName);
        Charset charset = StandardCharsets.UTF_8;

        String content = Files.readString(path, charset);
        content = content.replaceAll(regex, replacement);
        Files.writeString(path, content, charset);
    }

    public String toString(File file) throws Exception {

        return org.apache.commons.io.FileUtils.readFileToString(file, StandardCharsets.UTF_8);
    }
}
