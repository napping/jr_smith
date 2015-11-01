package edu.upenn.cis455.mapreduce.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.LinkedList;

/**
 * @author brishi
 */
public class Utils {

    public static LinkedList<BufferedReader> getFileReaders(File directory)
            throws FileNotFoundException
    {
        LinkedList<BufferedReader> readers = new LinkedList<>();

        for (File f : directory.listFiles()) {
            if (f.isFile() && f.getName().charAt(0) != '.') {
                readers.add(new BufferedReader(new FileReader(f)));
            }
        }

        return readers;
    }
}
