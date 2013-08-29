package net.trevize.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;

/**
 * 
 * @author Nicolas James <nicolas.james@gmail.com> [[http://njames.trevize.net]]
 *
 */

public class FileUtils {
	/**
	 * This method add a header to a file.
	 * @param header the header to add.
	 * @param f0 the file.
	 */
	public static void addHeaderToFile(String header, File f0) {
		String dirpath = null;
		try {
			dirpath = f0.getParentFile().getCanonicalPath();
		} catch (IOException e) {
			e.printStackTrace();
		}
		String entryFileName = f0.getName();
		String resultFileName = "TMP_" + new Date().getTime() + "_"
				+ f0.getName();

		//create the result file.
		File f1 = new File(dirpath, resultFileName);

		//create a FileWriter and a BufferedReader on the result file.
		FileWriter fw1 = null;
		try {
			fw1 = new FileWriter(f1);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		BufferedWriter bw1 = new BufferedWriter(fw1);

		//add the header to the result file.
		try {
			bw1.write(header);
			bw1.flush(); //do not forget, very important !
		} catch (IOException e) {
			e.printStackTrace();
		}

		//copy the content of f0 in the result file.
		try {
			FileReader fr0 = new FileReader(f0);
			char[] buf = new char[1024];
			int len;
			while ((len = fr0.read(buf)) > 0) {
				fw1.write(buf, 0, len);
			}
			fr0.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		//close the BufferedReader and the FileReader on the result file.
		try {
			bw1.close();
			fw1.close();
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		//delete the entry file.
		f0.delete();

		f1.renameTo(new File(dirpath, entryFileName));
	}
}
