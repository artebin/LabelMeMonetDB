/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008- MonetDB B.V.
 * All Rights Reserved.
 */

package nl.cwi.monetdb.xquery.xrpc.api;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.PostMethod;

/**
 * sendReceive(String, String) method modified by Nicolas James
 * 2009.01.20
 */

/**
 * This class contains functions to send and/or receive (XRPC) messages
 * using HTTP connections.
 *
 * @author Ying Zhang <Y.Zhang@cwi.nl>
 * @version 0.1
 */

public class XRPCHTTPConnection {
	/**
	 * HTTP response header: 200 OK
	 */
	public static final String HTTP_OK_HEADER = "HTTP/1.1 200 OK\r\n"
			+ "Content-Type: text/xml; charset=\"utf-8\"\r\n\r\n";
	/**
	 * HTTP response header: 400 Bad Request
	 */
	public static final String HTTP_ERR_400_HEADER = "HTTP/1.1 400 Bad Request\r\n"
			+ "Content-Type: text/xml; charset=\"utf-8\"\r\n\r\n";
	/**
	 * HTTP response header: 500 Internal Server Error
	 */
	public static final String HTTP_ERR_500_HEADER = "HTTP/1.1 500 Internal Server Error\r\n"
			+ "Content-Type: text/xml; charset=\"utf-8\"\r\n\r\n";

	/**
	 * HTTP request type GET
	 */
	public static final int HTTP_METHOD_GET = 0;

	/**
	 * HTTP request type POST
	 */
	public static final int HTTP_METHOD_POST = 1;

	/**
	 * Sends the given (XRPC) message <code>msg</code> in the pay load
	 * of an HTTP message with <code>httpHeader</code> using the
	 * <code>writer</code>.
	 * It is up to the caller to close <code>writer</code>.
	 *
	 * @param writer A (socket) writer for sending message
	 * @param httpHeader Header of the HTTP message
	 * @param msg The message to be send as the pay load of the HTTP
	 * message.
	 * @throws IOException If an I/O error occurs
	 */
	public static void send(BufferedWriter writer, String httpHeader, String msg)
			throws IOException {
		writer.write(httpHeader);
		writer.write(msg);
		writer.flush();
	}

	/**
	 * Reads an HTTP message using the <code>socketReader</code> and
	 * checks the HTTP header for mandatory fields, the validity of the
	 * values of the mandatory fields.
	 * The header is then discarded.
	 * If the request is an HTTP POST request, returns the XRPC request
	 * message included in the pay load as a <code>String</code>.
	 * If the request if an HTTP GET request, returns the GET URL
	 * <code>String</code>.
	 *
	 * It is up to the caller to close the socket and the associated
	 * streams.
	 *
	 * FIXME: since we now expect two kinds of request, nl. XRPC request
	 * and HTTP GET requst, the return type of this function should be
	 * changed to return both message type (XRPC_REQ, HTTP_GET, or
	 * error) and the message.
	 *
	 * @param socketReader A reader based on socket
	 * @param reqURI Accepted URI in the HTTP request header
	 * @return the received (XRPC) message
	 * @throws XRPCSenderException If the received message is invalid
	 * @throws XRPCReceiverException If the receiver failed to read the
	 * whole message
	 */
	public static String receive(BufferedReader socketReader, String reqURI)
			throws XRPCSenderException, XRPCReceiverException {
		int contentLength = 0;
		String ln = null, getURI = null;
		boolean foundReqHeader = false;
		boolean foundClHeader = false;
		boolean foundHostHeader = false;
		int reqType = HTTP_METHOD_POST;

		/* read the HTTP header */
		try {
			while (((ln = socketReader.readLine()) != null)
					&& (ln.length() > 0)) {
				if (ln.startsWith("GET")) {
					reqType = HTTP_METHOD_GET;
					foundReqHeader = true;
					int urlEnd = ln.indexOf(" ", 4);
					getURI = urlEnd > 4 ? ln.substring(4, urlEnd) : "";
					foundClHeader = true;
				} else if (ln.startsWith("POST")) {
					if (!ln.startsWith(reqURI, 5)) {
						throw new XRPCSenderException("Unsupported Request: \""
								+ ln + "\"");
					}
					foundReqHeader = true;
				} else if (ln.startsWith("Content-Length:")) {
					contentLength = Integer.parseInt(ln.substring(16));
					foundClHeader = true;
				} else if (ln.startsWith("Host:")) {
					/* TODO: save the value of Host for later use??? */
					foundHostHeader = true;
				}
			}
		} catch (NumberFormatException nfe) {
			throw new XRPCSenderException("Invalid value of "
					+ "\"Content-Length\": \"" + ln.substring(16) + "\": "
					+ nfe.getMessage());
		} catch (IOException ioe) {
			throw new XRPCSenderException("Could not read HTTP request header.");
		}

		if (!foundReqHeader) {
			throw new XRPCSenderException("HTTP header does not contain "
					+ "a \"POST\" method definition.");
		} else if (!foundClHeader) {
			throw new XRPCSenderException("HTTP header does not contain "
					+ "the mandatory \"Content-Length\" field.");
		} else if (!foundHostHeader) {
			throw new XRPCSenderException("HTTP header does not contain "
					+ "the mandatory \"Host\" field.");
		}

		if (reqType == HTTP_METHOD_GET)
			return getURI;

		/* read the XRPC request message */
		StringBuffer reqMsg = new StringBuffer(contentLength + 10);
		ln = null;
		try {
			while ((reqMsg.length() < contentLength)
					&& ((ln = socketReader.readLine()) != null)) {
				reqMsg.append(ln).append('\n');
			}
		} catch (IOException e) {
			throw new XRPCReceiverException("Failed to receive request.");
		}

		if (reqMsg.length() != contentLength) {
			throw new XRPCReceiverException("Failed to receive all request: "
					+ contentLength + "bytes expected, " + reqMsg.length()
					+ " bytes received.");
		}

		return reqMsg.toString();
	}

	/** 
	 * Sends the given XRPC request over an HTTP connection 
	 * to the destination server and returns the server's 

	 * response message, which can be an XRPC response message or a SOAP 
	 * Fault message, in a StringBuffer. 
	 * 
	 * @param server URL of the destination XRPC server 
	 * @param request The (XRPC) request to send 
	 * @return Server's response message, which can be an XRPC response 
	 * message or a SOAP Fault message. 
	 * @throws IOException If an I/O error occurs 
	 */
	public static StringBuffer sendReceive(String server, String request)
			throws IOException {
		URL url = new URL(server);

		PostMethod pmethod = new PostMethod(server);

		//        HttpURLConnection httpConn = (HttpURLConnection) url.openConnection(); 
		//        httpConn.setDoInput(true); 
		//        httpConn.setDoOutput(true); 
		//        httpConn.setUseCaches(false); 
		//        httpConn.setRequestMethod("POST"); 

		//        httpConn.setRequestProperty("Content-Type", 
		//                                    "application/x-www-form-urlencoded"); 

		pmethod.setRequestHeader("Content-Type",
				"application/x-www-form-urlencoded");

		//        httpConn.setRequestProperty("Content-Length", request.length()+""); 
		//        httpConn.connect(); 

		/* Send POST output. */
		//        DataOutputStream printout = new 
		//            DataOutputStream(httpConn.getOutputStream()); 
		//        printout.writeBytes(request); 
		//        printout.flush (); 
		pmethod.setRequestBody(request);

		HttpClient client = new HttpClient();
		int status = client.executeMethod(pmethod);

		StringBuffer response = new StringBuffer();

		InputStreamReader is = new InputStreamReader(pmethod
				.getResponseBodyAsStream());
		char[] buf = new char[1024];
		int len;
		while ((len = is.read(buf, 0, 1024)) > 0) {
			response.append(buf, 0, len);
		}

		/* Get response data. */
		//        InputStreamReader isReader; 
		//        StringBuffer response = new StringBuffer(16384); 
		//        if(httpConn.getResponseCode() != HttpURLConnection.HTTP_OK){ 
		//		System.out.println(httpConn.getResponseCode()); 
		//		System.out.println(httpConn.getResponseMessage()); 
		/* Read the SOAP Fault message. */
		//            isReader = new InputStreamReader(httpConn.getErrorStream()); 
		//        } else { 
		/* Read the response message, which can also be a SOAP Fault 
		 * message. */
		//            isReader = new InputStreamReader(httpConn.getInputStream()); 
		//        } 
		//        int c; 
		//        while( (c = isReader.read()) >= 0) response.append((char)c); 
		//        isReader.close(); 
		//        printout.close();
		return response;
	}
}
