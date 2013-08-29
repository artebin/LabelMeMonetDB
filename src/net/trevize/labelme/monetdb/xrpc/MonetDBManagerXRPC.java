package net.trevize.labelme.monetdb.xrpc;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;

import net.trevize.utils.FileComparator;
import nl.cwi.monetdb.xquery.xrpc.api.XRPCHTTPConnection;
import nl.cwi.monetdb.xquery.xrpc.api.XRPCMessage;

/**
 * This class provides a XRPC MonetDB manager for the LabelMe annotations dataset.
 * See [[http://njames.trevize.net/wiki/doku.php/monetdb]].
 * Three method is this class:
 * 		1. one-message per annotation file
 * 		2. one-message per directory
 * 		3. one-message for all the dataset (-Xmx512m required)
 * 
 * TODO: 
 * 		* could be more homogeneous to declare all function in pf with user-defined
 * XQuery function in the labelme XQuery module (like pf:add-doc and pf:del-doc).
 * 
 * @author Nicolas James <nicolas.james@gmail.com> [[http://njames.trevize.net]]
 * MonetDBManagerXRPC.java - Jan 8, 2009
 */

public class MonetDBManagerXRPC {
	//the path to ~LabelMe/database/annotations
	private String dir_dataset_path;

	//the URL to ~LabelMe/database/annotations
	private String url_dataset;

	//a prefix used in the document name (during the indexing)
	private int nbdoc;

	/**
	 * Indexing with a "one-message per annotation file" method.
	 * 
	 * @param dir_dataset_path the path to ~LabelMe/database/annotations
	 * @param url_dataset the URL to ~LabelMe/database/annotations
	 */
	public void indexDataset1(String dir_dataset_path, String url_dataset) {
		System.out.println("indexDataset1: Begin indexDataset.");

		this.dir_dataset_path = dir_dataset_path;
		this.url_dataset = url_dataset;

		nbdoc = 0;

		indexDir("");

		System.out.println("indexDataset1: End indexDataset.");
	}

	/**
	 * This method is called by indexDataset1(String, String).
	 * 
	 * @param dir_path a directory name in ~LabelMe/database/annotations
	 */
	private void indexDir(String dir_path) {
		System.out.println("Storing in MonetDB");
		System.out.println("\tfor directory: " + dir_dataset_path + "/"
				+ dir_path);

		File[] lf = new File(dir_dataset_path + "/" + dir_path).listFiles();

		//inserting using a lexicographic order on directory names. 
		Arrays.sort(lf, new FileComparator()); //only works with Java1.6.

		for (File f : lf) {
			if (f.isDirectory()) {
				indexDir(dir_path + "/" + f.getName());

			} else {
				if (f.getName().endsWith(".xml")) {
					indexFile(dir_path, f.getName());
				}
			}
		}
	}

	/**
	 * This method is called by indexDir(String).
	 * 
	 * @param dir_path a directory name in ~LabelMe/database/annotations
	 * @param file_name a filename of a file contained in a directory contained in ~LabelMe/database/annotations
	 */
	private void indexFile(String dir_path, String file_name) {
		//generate the request message for this file.
		StringBuffer callBody = new StringBuffer();

		String file_path = dir_dataset_path + "/" + dir_path + "/" + file_name;

		String file_url = url_dataset + "/" + dir_path + "/" + file_name;

		//I need the dir_path name to build the collection name.
		String dir_path_name = new File(dir_path).getName();

		//if specifying the collection name.
		callBody.append(XRPCMessage.XRPC_CALL(XRPCMessage.XRPC_SEQ(XRPCMessage
				.XRPC_ATOM("string", file_url))

				+ XRPCMessage.XRPC_SEQ(XRPCMessage.XRPC_ATOM("string", nbdoc
						+ "_" + file_name))

				+ XRPCMessage.XRPC_SEQ(XRPCMessage.XRPC_ATOM("string",
						"labelme_" + dir_path_name))));

		int arity = 3;
		int iterc = 1;

		String reqHeader = XRPCMessage
				.XRPC_REQ_HEADER(
						"labelme" /* namespace URI of the XQuery module */,
						"http://localhost/~nicolas/LabelMe.xq" /* location where the module file is stored */,
						"addAnnotationFile" /* the called XQuery function */,
						arity /* number of parameter the called function has */,
						iterc /* number of iterations the called function should be executed */,
						"true" /* indicate the called function is read-only or updating */

						/* note that the pathfinder document management function (e.g. pf:add-doc
						 * is also considered to be updating functions by XRPC */,

						"none-trace" /* query execution mode */, "");

		String reqMsg = XRPCMessage.SOAP_REQUEST("", /* soapHeader */
		reqHeader, callBody.toString());

		//sendReceive the message.
		StringBuffer respMsg = null;
		try {
			//System.out.println("indexing: "+file_path);
			respMsg = XRPCHTTPConnection.sendReceive(
					"http://localhost:50001/xrpc/", reqMsg);
		} catch (IOException e) {
			System.out.println("Error: ");
			System.out.println("\tdirectory: " + dir_path);
			System.out.println("\tfile: " + file_path);
			e.printStackTrace();
		}

		//print the response.
		System.out.println(respMsg.toString());

		nbdoc++;
	}

	/**
	 * Indexing the dataset with a "one-message per directory" method.
	 * This is the recommended method for indexing the LabelMe dataset.
	 * 
	 * @param dir_dataset_path the path to ~LabelMe/database/annotations
	 * @param url_dataset the URL to ~LabelMe/database/annotations
	 */
	public void indexDataset2(String dir_dataset_path, String url_dataset) {
		System.out.println("indexDataset2: Begin indexDataset.");

		this.dir_dataset_path = dir_dataset_path;
		this.url_dataset = url_dataset;

		nbdoc = 0;

		File[] dl = new File(dir_dataset_path).listFiles();

		/*
		 * inserting in MonetDB using a lexicographic order on directory names,
		 * (debugging easier). 
		 */
		Arrays.sort(dl, new FileComparator()); //only works with Java1.6.

		//for all directories in the dataset.
		for (File d : dl) {
			File[] al = d.listFiles();

			//go to next iteration if empty directory.
			if (al.length == 0) {
				continue;
			}

			/*
			 * Generating parameters for the XQuery
			 * labelme:addAnnotationFiles($uri as xs:string+, $doc as xs:string+, $col as xs:string) 
			 * function.
			 */
			StringBuffer files_url = new StringBuffer();
			StringBuffer files_name = new StringBuffer();

			for (File a : al) {

				String file_url = url_dataset + "/" + d.getName() + "/"
						+ a.getName();

				files_url.append(XRPCMessage.XRPC_ATOM("string", file_url));

				files_name.append(XRPCMessage.XRPC_ATOM("string", nbdoc + "_"
						+ a.getName()));

				nbdoc++;

			}

			//generate the request message for this directory.
			StringBuffer callBody = new StringBuffer();

			//if specifying the collection name.
			callBody.append(XRPCMessage.XRPC_CALL(XRPCMessage
					.XRPC_SEQ(files_url.toString())

					+ XRPCMessage.XRPC_SEQ(files_name.toString())

					+ XRPCMessage.XRPC_SEQ(XRPCMessage.XRPC_ATOM("string",
							"labelme_" + d.getName()))));

			int arity = 3;
			int iterc = 1;

			String reqHeader = XRPCMessage
					.XRPC_REQ_HEADER(
							"labelme" /* namespace URI of the XQuery module */,
							"http://localhost/~nicolas/LabelMe.xq" /* location where the module file is stored */,
							"addAnnotationFiles" /* the called XQuery function */,
							arity /* number of parameter the called function has */,
							iterc /* number of iterations the called function should be executed */,
							"true" /* indicate the called function is read-only or updating */

							/* note that the pathfinder document management function (e.g. pf:add-doc
							 * is also considered to be updating functions by XRPC */,

							"none-trace" /* query execution mode */, "");

			String reqMsg = XRPCMessage.SOAP_REQUEST("", /* soapHeader */
			reqHeader, callBody.toString());

			//sendReceive the message.
			StringBuffer respMsg = null;
			try {
				respMsg = XRPCHTTPConnection.sendReceive(
						"http://localhost:50001/xrpc/", reqMsg);
			} catch (IOException e) {
				System.out.println("Error in the directory " + d.getName());
				e.printStackTrace();
			}

			//print the response.
			System.out.println(respMsg.toString());
		}

		System.out.println("indexDataset2: End indexDataset.");
	}

	/**
	 * Indexing the dataset using a "one-message for all the dataset" method.
	 * 
	 * @param dir_dataset_path path to ~LabelMe/database/annotations
	 * @param url_dataset URL to ~LabelMe/database/annotations
	 */
	public void indexDataset3(String dir_dataset_path, String url_dataset) {
		System.out.println("indexDataset3: Begin indexDataset.");

		this.dir_dataset_path = dir_dataset_path;
		this.url_dataset = url_dataset;

		nbdoc = 0;

		File[] dl = new File(dir_dataset_path).listFiles();

		/*
		 * inserting in MonetDB using a lexicographic order on directory names,
		 * (debugging easier). 
		 */
		Arrays.sort(dl, new FileComparator()); //only works with Java1.6.

		/*
		 * Generating parameters for the XQuery
		 * labelme:addAnnotationFiles($uri as xs:string+, $doc as xs:string+, $col as xs:string) 
		 * function.
		 */
		StringBuffer files_url = new StringBuffer();
		StringBuffer files_name = new StringBuffer();
		StringBuffer cols_name = new StringBuffer();

		//for all directories in the dataset.
		for (File d : dl) {
			File[] al = d.listFiles();

			//go to next iteration if empty directory.
			if (al.length == 0) {
				continue;
			}

			for (File a : al) {

				String file_url = url_dataset + "/" + d.getName() + "/"
						+ a.getName();

				files_url.append(XRPCMessage.XRPC_ATOM("string", file_url));

				files_name.append(XRPCMessage.XRPC_ATOM("string", nbdoc + "_"
						+ a.getName()));

				cols_name.append(XRPCMessage.XRPC_ATOM("string", "labelme_"
						+ a.getName()));

				nbdoc++;

			}

		}

		//generate the request message for this directory.
		StringBuffer callBody = new StringBuffer();

		//if specifying the collection name.
		callBody.append(XRPCMessage.XRPC_CALL(XRPCMessage.XRPC_SEQ(files_url
				.toString())

				+ XRPCMessage.XRPC_SEQ(files_name.toString())

				+ XRPCMessage.XRPC_SEQ(cols_name.toString())));

		int arity = 3;
		int iterc = 1;

		String reqHeader = XRPCMessage
				.XRPC_REQ_HEADER(
						"labelme" /* namespace URI of the XQuery module */,
						"http://localhost/~nicolas/LabelMe.xq" /* location where the module file is stored */,
						"addAnnotationFiles2" /* the called XQuery function */,
						arity /* number of parameter the called function has */,
						iterc /* number of iterations the called function should be executed */,
						"true" /* indicate the called function is read-only or updating */

						/* note that the pathfinder document management function (e.g. pf:add-doc
						 * is also considered to be updating functions by XRPC */,

						"none-trace" /* query execution mode */, "");

		String reqMsg = XRPCMessage.SOAP_REQUEST("", /* soapHeader */
		reqHeader, callBody.toString());

		//sendReceive the message.
		StringBuffer respMsg = null;
		try {
			respMsg = XRPCHTTPConnection.sendReceive(
					"http://localhost:50001/xrpc/", reqMsg);
		} catch (IOException e) {
			e.printStackTrace();
		}

		//print the response.
		System.out.println(respMsg.toString());

		System.out.println("indexDataset3: End indexDataset.");
	}

	/**
	 * 
	 * @param annotation
	 */
	public void searchByAnnotation(String annotation) {
		System.out.println("searchByAnnotation: begin.");

		//generate the request message for this directory.
		StringBuffer callBody = new StringBuffer();

		//if specifying the collection name.
		callBody.append(XRPCMessage.XRPC_CALL(XRPCMessage.XRPC_SEQ(XRPCMessage
				.XRPC_ATOM("string", annotation))));

		int arity = 1;
		int iterc = 1;

		String reqHeader = XRPCMessage
				.XRPC_REQ_HEADER(
						"labelme" /* namespace URI of the XQuery module */,
						"http://localhost/~nicolas/LabelMe.xq" /* location where the module file is stored */,
						"searchByAnnotation" /* the called XQuery function */,
						arity /* number of parameter the called function has */,
						iterc /* number of iterations the called function should be executed */,
						"true" /* indicate the called function is read-only or updating */

						/* note that the pathfinder document management function (e.g. pf:add-doc
						 * is also considered to be updating functions by XRPC */,

						"none-trace" /* query execution mode */, "" /* the caller */);

		String reqMsg = XRPCMessage.SOAP_REQUEST("", /* soapHeader */
		reqHeader, callBody.toString());

		//sendReceive the message.
		StringBuffer respMsg = null;
		try {
			respMsg = XRPCHTTPConnection.sendReceive(
					"http://localhost:50001/xrpc/", reqMsg);
		} catch (IOException e) {
			e.printStackTrace();
		}

		//print the response.
		//System.out.println(respMsg.toString());

		try {
			FileWriter fw = new FileWriter("searchByAnnotation.log");
			BufferedWriter bw = new BufferedWriter(fw);
			bw.write(respMsg.toString());
			bw.flush();
			fw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		System.out.println("searchByAnnotation: end.");
	}

	/**
	 * Insert a XML node at the end of a document.
	 * @param filename the file in which adding the new XML fragment.
	 * @param node the XML fragment to insert.
	 */
	public void insertNodeAsLast(String filename, String node) {
		System.out.println("insertNodeAsLast: begin.");

		//generate the request message for this directory.
		StringBuffer callBody = new StringBuffer();

		callBody.append(XRPCMessage.XRPC_CALL(

		XRPCMessage.XRPC_SEQ(XRPCMessage.XRPC_ATOM("string", filename)) +

		XRPCMessage.XRPC_SEQ(XRPCMessage.XRPC_ELEMENT(node))));

		int arity = 2;
		int iterc = 1;

		String reqHeader = XRPCMessage
				.XRPC_REQ_HEADER(
						"labelme" /* namespace URI of the XQuery module */,
						"http://localhost/~nicolas/LabelMe.xq" /* location where the module file is stored */,
						"addVisualInstance" /* the called XQuery function */,
						arity /* number of parameter the called function has */,
						iterc /* number of iterations the called function should be executed */,
						"true" /* indicate the called function is read-only or updating */

						/* note that the pathfinder document management function (e.g. pf:add-doc
						 * is also considered to be updating functions by XRPC */,

						"none-trace" /* query execution mode */, "" /* the caller */);

		String reqMsg = XRPCMessage.SOAP_REQUEST("", /* soapHeader */
		reqHeader, callBody.toString());

		//sendReceive the message.
		StringBuffer respMsg = null;
		try {
			respMsg = XRPCHTTPConnection.sendReceive(
					"http://localhost:50001/xrpc/", reqMsg);
		} catch (IOException e) {
			e.printStackTrace();
		}

		//print the response.
		System.out.println(respMsg.toString());

		System.out.println("addVisualInstance: end.");
	}

	public void addDocument(String url, String filename, String collection) {
		//generate the request message for this file.
		StringBuffer callBody = new StringBuffer();

		//if specifying the collection name.
		callBody.append(XRPCMessage.XRPC_CALL(XRPCMessage.XRPC_SEQ(XRPCMessage
				.XRPC_ATOM("string", url))

				+ XRPCMessage.XRPC_SEQ(XRPCMessage
						.XRPC_ATOM("string", filename))

				+ XRPCMessage.XRPC_SEQ(XRPCMessage.XRPC_ATOM("string",
						collection))));

		int arity = 3;
		int iterc = 1;

		String reqHeader = XRPCMessage
				.XRPC_REQ_HEADER(
						"labelme" /* namespace URI of the XQuery module */,
						"http://localhost/~nicolas/LabelMe.xq" /* location where the module file is stored */,
						"addAnnotationFile" /* the called XQuery function */,
						arity /* number of parameter the called function has */,
						iterc /* number of iterations the called function should be executed */,
						"true" /* indicate the called function is read-only or updating */

						/* note that the pathfinder document management function (e.g. pf:add-doc
						 * is also considered to be updating functions by XRPC */,

						"none-trace" /* query execution mode */, "");

		String reqMsg = XRPCMessage.SOAP_REQUEST("", /* soapHeader */
		reqHeader, callBody.toString());

		//sendReceive the message.
		StringBuffer respMsg = null;
		try {
			//System.out.println("indexing: "+file_path);
			respMsg = XRPCHTTPConnection.sendReceive(
					"http://localhost:50001/xrpc/", reqMsg);
		} catch (IOException e) {
			e.printStackTrace();
		}

		//print the response.
		System.out.println(respMsg.toString());
	}




	public void deleteDocument(String filename) {
		System.out.println("deleteDocument: begin.");

		//generate the request message for this directory.
		StringBuffer callBody = new StringBuffer();

		//if specifying the collection name.
		callBody.append(XRPCMessage.XRPC_CALL(

		XRPCMessage.XRPC_SEQ(XRPCMessage.XRPC_ATOM("string", filename))));

		int arity = 1;
		int iterc = 1;

		String reqHeader = XRPCMessage
				.XRPC_REQ_HEADER(
						"labelme" /* namespace URI of the XQuery module */,
						"http://localhost/~nicolas/LabelMe.xq" /* location where the module file is stored */,
						"deleteDocument" /* the called XQuery function */,
						arity /* number of parameter the called function has */,
						iterc /* number of iterations the called function should be executed */,
						"true" /* indicate the called function is read-only or updating */

						/* note that the pathfinder document management function (e.g. pf:add-doc
						 * is also considered to be updating functions by XRPC */,

						"none-trace" /* query execution mode */, "" /* the caller */);

		String reqMsg = XRPCMessage.SOAP_REQUEST("", /* soapHeader */
		reqHeader, callBody.toString());

		//sendReceive the message.
		StringBuffer respMsg = null;
		try {
			respMsg = XRPCHTTPConnection.sendReceive(
					"http://localhost:50001/xrpc/", reqMsg);
		} catch (IOException e) {
			e.printStackTrace();
		}

		//print the response.
		System.out.println(respMsg.toString());

		System.out.println("deleteDocument: end.");
	}





	public void getAnnotations() {
		System.out.println("getAnnotations: begin.");

		//generate the request message for this directory.
		StringBuffer callBody = new StringBuffer();

		//no arguments for this function.
		callBody.append(XRPCMessage.XRPC_CALL(null));

		int arity = 0;
		int iterc = 1;

		String reqHeader = XRPCMessage
				.XRPC_REQ_HEADER(
						"labelme" /* namespace URI of the XQuery module */,
						"http://localhost/~nicolas/LabelMe.xq" /* location where the module file is stored */,
						"getAnnotations" /* the called XQuery function */,
						arity /* number of parameter the called function has */,
						iterc /* number of iterations the called function should be executed */,
						"false" /* indicate the called function is read-only or updating */

						/* note that the pathfinder document management function (e.g. pf:add-doc
						 * is also considered to be updating functions by XRPC */,

						"none-trace" /* query execution mode */, "" /* the caller */);

		String reqMsg = XRPCMessage.SOAP_REQUEST("", /* soapHeader */
		reqHeader, callBody.toString());

		//sendReceive the message.
		StringBuffer respMsg = null;
		try {
			respMsg = XRPCHTTPConnection.sendReceive(
					"http://localhost:50001/xrpc/", reqMsg);
		} catch (IOException e) {
			e.printStackTrace();
		}

		//print the response.
		//System.out.println(respMsg.toString());

		try {
			FileWriter fw = new FileWriter("getAnnotations.log");
			BufferedWriter bw = new BufferedWriter(fw);
			bw.write(respMsg.toString());
			bw.flush();
			fw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		System.out.println("getAnnotations: end.");
	}

	public static void main(String args[]) {
		String dir_dataset_path = "/home/nicolas/public_html/LabelMe/database/annotations";

		String url_dataset = "http://localhost/~nicolas/LabelMe/database/annotations";

		MonetDBManagerXRPC mdbm = new MonetDBManagerXRPC();

//		mdbm.indexDataset2(dir_dataset_path, url_dataset);

		//				mdbm.searchByAnnotation("car");

		//				mdbm.searchByAnnotation("roast chicken");

		//				mdbm.addVisualInstance("1_p1010843.xml",
		//						"<visualInstances><instance>toto</instance></visualInstances>");

		//mdbm.deleteDocument("1_p1010843.xml");

		//		mdbm
		//				.addDocument(
		//						"http://localhost/~nicolas/LabelMe/database/annotations/05june05_static_indoor/p1010843.xml",
		//						"1_p1010843.xml", "labelme_05june05_static_indoor");

				mdbm.getAnnotations();
	}
}
