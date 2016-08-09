package com.rogrand.fastdfs.client.cmd.storage;

import java.io.UnsupportedEncodingException;

import com.rogrand.fastdfs.client.cmd.AbstractCmd;
import com.rogrand.fastdfs.client.util.MyException;
import com.rogrand.fastdfs.client.util.ProtoCommon;
import com.rogrand.fastdfs.client.util.ProtoCommon.RecvPackageInfo;

import io.netty.channel.Channel;

public class TruncateFileCmd extends AbstractCmd<Integer> {

	private String groupName;
	private String appenderFilename;
	private long truncatedFileSize;

	public TruncateFileCmd(String groupName, String appenderFilename, long truncatedFileSize) {
		this.groupName = groupName;
		this.appenderFilename = appenderFilename;
		this.truncatedFileSize = truncatedFileSize;
		this.responseCmd = ProtoCommon.STORAGE_PROTO_CMD_RESP;
		this.responseSize = 0;
	}

	@Override
	protected void doRequest(Channel channel) throws UnsupportedEncodingException {
		byte[] header;
		byte[] hexLenBytes;
		byte[] appenderFilenameBytes;
		int offset;
		int body_len;

		if ((groupName == null || groupName.length() == 0) || (appenderFilename == null || appenderFilename.length() == 0)) {
			throw new MyException("groupName和appenderFilename均不能为空");
		}
		appenderFilenameBytes = appenderFilename.getBytes(ProtoCommon.CHARSET);
		body_len = 2 * ProtoCommon.FDFS_PROTO_PKG_LEN_SIZE + appenderFilenameBytes.length;

		header = ProtoCommon.packHeader(ProtoCommon.STORAGE_PROTO_CMD_TRUNCATE_FILE, body_len, (byte) 0);
		byte[] wholePkg = new byte[header.length + body_len];
		System.arraycopy(header, 0, wholePkg, 0, header.length);
		offset = header.length;

		hexLenBytes = ProtoCommon.long2buff(appenderFilename.length());
		System.arraycopy(hexLenBytes, 0, wholePkg, offset, hexLenBytes.length);
		offset += hexLenBytes.length;

		hexLenBytes = ProtoCommon.long2buff(truncatedFileSize);
		System.arraycopy(hexLenBytes, 0, wholePkg, offset, hexLenBytes.length);
		offset += hexLenBytes.length;

		System.arraycopy(appenderFilenameBytes, 0, wholePkg, offset, appenderFilenameBytes.length);
		offset += appenderFilenameBytes.length;

		channel.writeAndFlush(wholePkg);
	}

	@Override
	protected Integer getResponse(RecvPackageInfo response) {
		return (int) response.errno;
	}

}
