package net.trevize.labelme.monetdb.jdbc;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;

public class MonetDBManagerJDBC {
	private Connection con;

	public MonetDBManagerJDBC() {
	}

	public void indexDataset(String dirpath) {
		try {
			// make sure the driver is loaded
			Class.forName("nl.cwi.monetdb.jdbc.MonetDriver");
			con = DriverManager
					.getConnection(
							"jdbc:monetdb://localhost:50000/database?language=xquery",
							"monetdb", "monetdb");

			indexDir(dirpath);

			con.close();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}

		System.out.println("End indexDataset.");
	}

	private void indexDir(String dirpath) {
		System.out.println("Storing in MonetDB for directory: " + dirpath);

		File[] lf = new File(dirpath).listFiles();

		for (File f : lf) {
			if (f.isDirectory()) {
				try {
					indexDir(f.getCanonicalPath());
				} catch (IOException e) {
					e.printStackTrace();
				}
			} else {
				if (f.getName().endsWith(".xml")) {
					try {
						indexFile(f.getCanonicalPath());
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		}
	}

	private void indexFile(String filepath) {
		try {
			Statement st = con.createStatement();

			FileReader fis = new FileReader(filepath);
			StringBuffer sbuf = new StringBuffer();
			char[] buf = new char[1024];
			int len;
			while ((len = fis.read(buf, 0, 1024)) > 0) {
				sbuf.append(buf, 0, len);
			}

			st.addBatch(sbuf.toString());

			st.executeBatch();

			/* The name of the document is written as warning to the
			 * Connection's warning stack.  This is kind of dirty, but since
			 * the batch cannot return a string, there is no other way here.
			*/
			SQLWarning w = con.getWarnings();
			while (w != null) {
				System.out.println(w.getMessage());
				w = w.getNextWarning();
			}

			st.close();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String args[]) {
		String dirpath = "/home/nicolas/dataset/LabelMe/database/annotations";

		MonetDBManagerJDBC mdbm = new MonetDBManagerJDBC();

		mdbm.indexDataset(dirpath);
	}
}
