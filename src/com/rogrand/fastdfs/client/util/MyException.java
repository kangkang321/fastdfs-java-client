/*
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS Java Client may be copied only under the terms of the GNU Lesser
* General Public License (LGPL).
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
*/

package com.rogrand.fastdfs.client.util;

/**
 * My Exception
 * 
 * @author Happy Fish / YuQing
 * @version Version 1.0
 */
public class MyException extends RuntimeException {

	private static final long serialVersionUID = 7488157452225830869L;

	public MyException(String message) {
		super(message);
	}

	public MyException(String message, Exception e) {
		super(message, e);
	}
}
