package edu.upenn.cis455.mapreduce.utils;

import java.io.*;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

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

    public static void newDir(File f) {
        if (!f.mkdir()) {
            deleteDir(f);
            f.mkdir();
        }
    }

    public static void deleteDir(File file) {
        File[] contents = file.listFiles();
        if (contents != null) {
            for (File f : contents) {
                deleteDir(f);
            }
        }
        file.delete();
    }

    public static List<BufferedWriter> getFileWriters(int numWorkers, String storageDirectory) {

		List<BufferedWriter> writers = new ArrayList<>();
		for (int i = 0; i < numWorkers; i++) {
			File file = new File(storageDirectory + "/spool-out/worker" + i);

			try {
				writers.add(new BufferedWriter(new FileWriter(file)));

			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		return writers;
	}
}

