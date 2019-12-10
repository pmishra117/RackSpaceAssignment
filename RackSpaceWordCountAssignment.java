package com.demo.wordcount;

import java.io.BufferedOutputStream;

import java.io.BufferedReader;

import java.io.File;

import java.io.FileNotFoundException;

import java.io.FileOutputStream;

import java.io.FileReader;

import java.io.IOException;

import java.io.RandomAccessFile;

import java.util.ArrayList;

import java.util.Collections;

import java.util.Comparator;

import java.util.HashMap;

import java.util.Iterator;

import java.util.List;

import java.util.Map;

import java.util.Scanner;

import java.util.Set;

import java.util.concurrent.Callable;

import java.util.concurrent.ConcurrentHashMap;

import java.util.concurrent.ExecutionException;

import java.util.concurrent.ExecutorService;

import java.util.concurrent.Executors;

import java.util.concurrent.Future;

import java.util.Map.Entry;

public class RackSpaceWordCountAssignment {

	static ConcurrentHashMap<String, Integer> result = null;

	static {

		result = new ConcurrentHashMap<String, Integer>();

	}

	public static void main(String[] args) throws Exception {

		System.out.println(

				" No of Available Processors   " + Runtime.getRuntime().availableProcessors() * 3 / 4);

		if (args.length > 0) {

			RandomAccessFile raf = new RandomAccessFile(args[0], "r");

			long sourceSize = raf.length();

			if (sourceSize > 1024) {

				splitFile(args[0]);

				File dir = new File(System.getProperty("user.dir"));

				System.out.println("Present working directory : " + System.getProperty("user.dir"));

				File[] fileList = dir.listFiles();

				for (File file : fileList) {

					if (file.isFile() && file.getName().startsWith("2600_")) {

						String filename = file.getName();

						System.out.println(file.getName());

						ExecutorService executor = Executors

								.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 3 / 4);

						Runnable worker = new WordCountRunnableImpl(filename);

						executor.submit(worker);

					}

				}

			} else {

				ExecutorService executor = Executors

						.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 3 / 4);

				Runnable worker = new WordCountRunnableImpl(args[0]);

				executor.submit(worker);

			}

		} else {

			throw new Exception("Args must have some values...");

		}

		System.out.println("Final Result with top 5 frequent words throught all chunks......");

		List<Entry<String, Integer>> resultSorted = sortResultByValueDesc(result);

		Iterator<Entry<String, Integer>> resultSortedItr = resultSorted.iterator();

		Long countTotal = 0l;

		while (resultSortedItr.hasNext()) {

			Entry<String, Integer> result = resultSortedItr.next();

			if (result.getKey().startsWith("2600_")) {

				countTotal += result.getValue();

			}

			System.out.println(result.getKey() + " count is ===> " + result.getValue());

		}

		System.out.println("Total words in All chunk of the files  " + countTotal);

	}

	// Splitting file if Its more than 1 MB

	public static void splitFile(String args) throws Exception {

		RandomAccessFile raf = new RandomAccessFile(args, "r");

		long sourceSize = raf.length(); // Total size of file

		long numSplits = sourceSize / 1024; // must be optimal I am using 1 mb per file split in this case

		long bytesPerSplit = sourceSize / numSplits;

		long remainingBytes = sourceSize % numSplits;

		System.out.println("No of spilits..." + numSplits);

		// System.out.println(

		// "Runtime.getRuntime().availableMemory " + Runtime.getRuntime().maxMemory() /
		// (1024 * 1024 * 1024));

		long diskSize = new File("/").getTotalSpace() / (1024 * 1024 * 1024);

		System.out.println("Space in disk   ....." + diskSize);

		int maxReadBufferSize = 1024; // 1Mb

		for (int destIx = 1; destIx <= numSplits; destIx++) {

			BufferedOutputStream bw = new BufferedOutputStream(new FileOutputStream("2600_" + destIx));

			if (bytesPerSplit > maxReadBufferSize) {

				long numReads = bytesPerSplit / maxReadBufferSize;

				long numRemainingRead = bytesPerSplit % maxReadBufferSize;

				for (int i = 0; i < numReads; i++) {

					readWrite(raf, bw, maxReadBufferSize);

				}

				if (numRemainingRead > 0) {

					readWrite(raf, bw, numRemainingRead);

				}

			} else {

				readWrite(raf, bw, bytesPerSplit);

			}

			bw.close();

		}

		if (remainingBytes > 0) {

			BufferedOutputStream bw = new BufferedOutputStream(new FileOutputStream("2600_" + (numSplits + 1)));

			readWrite(raf, bw, remainingBytes);

			bw.close();

		}

		raf.close();

	}

	static void readWrite(RandomAccessFile raf, BufferedOutputStream bw, long numBytes) throws IOException {

		byte[] buf = new byte[(int) numBytes];

		int val = raf.read(buf);

		if (val != -1) {

			bw.write(buf);

		}

	}

	// Sorting result by most frequent word in desc

	// Sorting by word count in descending order

	public static List<Entry<String, Integer>> sortResultByValueDesc(Map<String, Integer> resultMap) {

		Set<Entry<String, Integer>> keySet = resultMap.entrySet();

		List<Entry<String, Integer>> list = new ArrayList<>(keySet);

		Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {

			@Override

			public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {

				return (o2.getValue()).compareTo(o1.getValue());

			}

		});

		return list;

	}

}

class WordCountRunnableImpl implements Runnable {

	private String filename;

	ConcurrentHashMap<String, Integer> res = RackSpaceWordCountAssignment.result;

	volatile List<Entry<String, Integer>> list = null;

	public WordCountRunnableImpl(String filename) {

		this.filename = filename;

	}

	public void run() {

		list = sortByValueDesc(processFile(filename));

		Iterator<Entry<String, Integer>> listItr = list.iterator();

		System.out.println(Thread.currentThread().getName() + " File Chunk " + filename);

		System.out.println("Top 5 frequent words.....");

		int counter = 0;

		while (listItr.hasNext() && counter++ < 5) {

			Entry<String, Integer> data = listItr.next();

			System.out.println(data.getKey() + "====>  " + data.getValue() + "     ");

			if (!res.containsKey(data.getKey())) {

				res.put(data.getKey(), data.getValue());

			} else {

				// System.out.println(res.get(data.getKey()));

				// System.out.println(data.getValue());

				// System.out.println(res.get(data.getKey()) + data.getValue());

				res.put(data.getKey(), res.get(data.getKey()) + data.getValue());

			}

		}

	}

	// Processing file

	public synchronized HashMap<String, Integer> processFile(String filename) {

		int count = 0;

		HashMap<String, Integer> map = new HashMap<>();

		String junkChars = "['#','(',')','[',']','@','!','.',''','?',':',';','*','-']";

		BufferedReader br = null;

		String words[] = null;

		String line;

		try {

			br = new BufferedReader(new FileReader(filename)); // creates

			while (br.ready()) {

				String text = br.readLine();

				line = text.replaceAll("[^\\x00-\\x7F]", " ").replaceAll(junkChars, " ");

				words = line.split(" ");

				for (String word : words) {

					if (map.containsKey(word.trim())) {

						map.put(word, (map.get(word) + 1));

					} else {

						map.put(word.trim(), 1);

					}

				}

				count += words.length;

			}

			map.put(filename, count);

		} catch (IOException e) {

			// TODO Auto-generated catch block

			e.printStackTrace();

		}

		return map;

	}

	// Sorting by word count in descending order

	public synchronized List<Entry<String, Integer>> sortByValueDesc(Map<String, Integer> wordMap) {

		System.out.println("  Total words in " + filename + "\t   " + wordMap.get(filename));

		Set<Entry<String, Integer>> keySet = wordMap.entrySet();

		list = new ArrayList<>(keySet);

		Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {

			@Override

			public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {

				return (o2.getValue()).compareTo(o1.getValue());

			}

		});

		return list;

	}

}
