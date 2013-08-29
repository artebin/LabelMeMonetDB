package net.trevize.utils;

import java.io.File;
import java.text.Collator;
import java.util.Comparator;

public class FileComparator implements Comparator<File> {
	private Collator c = Collator.getInstance();

	@Override
	public int compare(File f1, File f2) {
		if (f1 == f2)
			return 0;

		return c.compare(f1.getName(), f2.getName());
	}

}
