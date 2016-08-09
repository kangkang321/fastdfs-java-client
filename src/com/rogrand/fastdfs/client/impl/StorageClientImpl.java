package com.rogrand.fastdfs.client.impl;

import java.io.FileInputStream;
import java.util.concurrent.ExecutionException;

import com.rogrand.fastdfs.client.StorageClient;
import com.rogrand.fastdfs.client.cmd.AbstractCmd;
import com.rogrand.fastdfs.client.cmd.storage.AppendFileCmd;
import com.rogrand.fastdfs.client.cmd.storage.DeleteFileCmd;
import com.rogrand.fastdfs.client.cmd.storage.DownloadFileCmd;
import com.rogrand.fastdfs.client.cmd.storage.GetMetadataCmd;
import com.rogrand.fastdfs.client.cmd.storage.ModifyFileCmd;
import com.rogrand.fastdfs.client.cmd.storage.QueryFileInfoCmd;
import com.rogrand.fastdfs.client.cmd.storage.SetMetadataCmd;
import com.rogrand.fastdfs.client.cmd.storage.TruncateFileCmd;
import com.rogrand.fastdfs.client.cmd.storage.UploadAppenderFileCmd;
import com.rogrand.fastdfs.client.cmd.storage.UploadFileCmd;
import com.rogrand.fastdfs.client.cmd.storage.UploadSlaveFileCmd;
import com.rogrand.fastdfs.client.model.FileInfo;
import com.rogrand.fastdfs.client.model.NameValuePair;
import com.rogrand.fastdfs.client.util.Base64;
import com.rogrand.fastdfs.client.util.MyException;
import com.rogrand.fastdfs.client.util.ProtoCommon;

import io.netty.channel.Channel;
import io.netty.channel.pool.ChannelPool;
import io.netty.util.concurrent.Future;

public class StorageClientImpl implements StorageClient {

	private final ChannelPool channelPool;

	private Base64 base64;

	public StorageClientImpl(ChannelPool channelPool) {
		this.channelPool = channelPool;
		this.base64 = new Base64();
	}

	public String[] upload_file(FileInputStream file, String fileExtName) throws InterruptedException {
		return execCommon(new UploadFileCmd(file, fileExtName));
	}

	public String[] upload_appender_file(FileInputStream file, String fileExtName) throws InterruptedException {
		return execCommon(new UploadAppenderFileCmd(file, fileExtName));
	}

	public int append_file(String groupName, String appenderFilename, FileInputStream file) throws InterruptedException {
		return execCommon(new AppendFileCmd(groupName, appenderFilename, file));
	}

	public int modify_file(String groupName, String appenderFilename, long fileOffset, FileInputStream file) throws InterruptedException {
		return execCommon(new ModifyFileCmd(groupName, appenderFilename, fileOffset, file));
	}

	public int delete_file(String groupName, String remoteFilename) throws InterruptedException {
		return execCommon(new DeleteFileCmd(groupName, remoteFilename));
	}

	public int truncate_file(String groupName, String appenderFilename) throws InterruptedException {
		return execCommon(new TruncateFileCmd(groupName, appenderFilename, 0));
	}

	public int truncate_file(String groupName, String appenderFilename, long truncatedFileSize) throws InterruptedException {
		return execCommon(new TruncateFileCmd(groupName, appenderFilename, truncatedFileSize));
	}

	public byte[] download_file(String groupName, String remoteFilename) throws InterruptedException {
		return execCommon(new DownloadFileCmd(groupName, remoteFilename, 0, 0));
	}

	public byte[] download_file(String groupName, String remoteFilename, long fileOffset, long downloadBytes) throws InterruptedException {
		return execCommon(new DownloadFileCmd(groupName, remoteFilename, fileOffset, downloadBytes));
	}

	public NameValuePair[] get_metadata(String groupName, String remoteFilename) throws InterruptedException {
		return execCommon(new GetMetadataCmd(groupName, remoteFilename));
	}

	public int set_metadata(String groupName, String remoteFilename, NameValuePair[] metaList, byte opFlag) throws InterruptedException {
		return execCommon(new SetMetadataCmd(groupName, remoteFilename, metaList, opFlag));
	}

	public FileInfo get_file_info(String groupName, String remoteFilename) throws InterruptedException {
		if (remoteFilename.length() < ProtoCommon.FDFS_FILE_PATH_LEN + ProtoCommon.FDFS_FILENAME_BASE64_LENGTH + ProtoCommon.FDFS_FILE_EXT_NAME_MAX_LEN + 1) {
			return null;
		}

		byte[] buff = base64.decodeAuto(remoteFilename.substring(	ProtoCommon.FDFS_FILE_PATH_LEN,
																	ProtoCommon.FDFS_FILE_PATH_LEN + ProtoCommon.FDFS_FILENAME_BASE64_LENGTH));

		long file_size = ProtoCommon.buff2long(buff, 4 * 2);
		if (((remoteFilename.length() > ProtoCommon.TRUNK_LOGIC_FILENAME_LENGTH)
				|| ((remoteFilename.length() > ProtoCommon.NORMAL_LOGIC_FILENAME_LENGTH) && ((file_size & ProtoCommon.TRUNK_FILE_MARK_SIZE) == 0)))
			|| ((file_size & ProtoCommon.APPENDER_FILE_SIZE) != 0)) { // slave file or appender file
			FileInfo fi = execCommon(new QueryFileInfoCmd(groupName, remoteFilename));
			if (fi == null) {
				return null;
			}
			return fi;
		}

		FileInfo fileInfo = new FileInfo(file_size, 0, 0, ProtoCommon.getIpAddress(buff, 0));
		fileInfo.setCreateTimestamp(ProtoCommon.buff2int(buff, 4));
		if ((file_size >> 63) != 0) {
			file_size &= 0xFFFFFFFFL; // low 32 bits is file size
			fileInfo.setFileSize(file_size);
		}
		fileInfo.setCrc32(ProtoCommon.buff2int(buff, 4 * 4));

		return fileInfo;
	}

	private <T> T execCommon(AbstractCmd<T> cmd) throws InterruptedException {
		Future<Channel> future = channelPool.acquire();
		future.sync();
		Channel channel;
		try {
			channel = future.get();
		}
		catch (ExecutionException e) {
			throw new MyException("未取得有效连接", e);
		}
		T result = cmd.execute(channel);
		channelPool.release(channel).sync();
		return result;
	}

	public String[] upload_slave_file(FileInputStream file, String masterFileId, String prefixName, String fileExtName) throws InterruptedException {
		return execCommon(new UploadSlaveFileCmd(file, masterFileId, fileExtName, prefixName));
	}
}
