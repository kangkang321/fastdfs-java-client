package com.rogrand.fastdfs;

import java.io.FileInputStream;

public interface FastdfsClient {

	public String[] upload_file(FileInputStream file, String fileExtName) throws InterruptedException;

	public String[] upload_appender_file(FileInputStream file, String fileExtName) throws InterruptedException;

	public String[] upload_slave_file(FileInputStream file, String masterFileId, String prefixName, String fileExtName) throws InterruptedException;

	public int append_file(String groupName, String appenderFilename, FileInputStream file) throws InterruptedException;

	public int modify_file(String groupName, String appenderFilename, long fileOffset, FileInputStream file) throws InterruptedException;

	public int delete_file(String groupName, String remoteFilename) throws InterruptedException;

	public String getUrl(String groupName, String remoteFilename) throws InterruptedException;

}
