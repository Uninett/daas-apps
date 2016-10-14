package com.github.sparkcaller.utils;

import java.io.*;

public class FileExtractor {

    public static String extractExecutable(String resourceFilePath) {

        if (!resourceFilePath.startsWith("/")) {
            resourceFilePath = "/" + resourceFilePath;
        }

        if (resourceFilePath == null)
            return null;

        try {
            // Read the file we're looking for
            InputStream fileStream = FileExtractor.class.getResourceAsStream(resourceFilePath);

            // Was the resource found?
            if (fileStream == null)
                return null;

            // Grab the file name
            String[] chopped = resourceFilePath.split("\\/");
            String fileName = chopped[chopped.length-1];

            // Create our temp file
            File tempFile = File.createTempFile("sparkcaller" + System.nanoTime(), fileName);

            // Delete the file on VM exit
            tempFile.deleteOnExit();

            OutputStream out = new FileOutputStream(tempFile);

            // Write the file to the temp file
            byte[] buffer = new byte[1024];
            int len = fileStream.read(buffer);
            while (len != -1) {
                out.write(buffer, 0, len);
                len = fileStream.read(buffer);
            }

            fileStream.close();
            out.close();

            tempFile.setExecutable(true);
            return tempFile.getAbsolutePath();

        } catch (IOException e) {
            return null;
        }
    }
}

