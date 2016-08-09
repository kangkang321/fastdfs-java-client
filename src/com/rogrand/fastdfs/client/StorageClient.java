package com.rogrand.fastdfs.client;

import java.io.FileInputStream;

import com.rogrand.fastdfs.client.model.FileInfo;
import com.rogrand.fastdfs.client.model.NameValuePair;

public interface StorageClient {

	public String[] upload_file(FileInputStream file, String fileExtName) throws InterruptedException;

	public String[] upload_appender_file(FileInputStream file, String fileExtName) throws InterruptedException;

	public String[] upload_slave_file(FileInputStream file, String masterFileId, String prefixName, String fileExtName) throws InterruptedException;

	public int append_file(String groupName, String appenderFilename, FileInputStream file) throws InterruptedException;

	public int modify_file(String groupName, String appenderFilename, long fileOffset, FileInputStream file) throws InterruptedException;

	public int delete_file(String groupName, String remoteFilename) throws InterruptedException;

	public int truncate_file(String groupName, String appenderFilename) throws InterruptedException;

	public int truncate_file(String groupName, String appenderFilename, long truncatedFileSize) throws InterruptedException;

	/**
	 * FIXME ChannelHandler的channelRead会莫名其妙的先执行完，然后再执行channelReadComplete；执行完channelReadComplete后又再次执行channelRead
	 * 
	 * @param groupName
	 * @param remoteFilename
	 * @return
	 * @throws InterruptedException
	 */
	@Deprecated
	public byte[] download_file(String groupName, String remoteFilename) throws InterruptedException;

	/**
	 * FIXME ChannelHandler的channelRead会莫名其妙的先执行完，然后再执行channelReadComplete；执行完channelReadComplete后又再次执行channelRead
	 * 
	 * @param groupName
	 * @param remoteFilename
	 * @return
	 * @throws InterruptedException
	 */
	@Deprecated
	public byte[] download_file(String groupName, String remoteFilename, long fileOffset, long downloadBytes) throws InterruptedException;

	public NameValuePair[] get_metadata(String groupName, String remoteFilename) throws InterruptedException;

	public int set_metadata(String groupName, String remoteFilename, NameValuePair[] metaList, byte opFlag) throws InterruptedException;

	public FileInfo get_file_info(String groupName, String remoteFilename) throws InterruptedException;

}
