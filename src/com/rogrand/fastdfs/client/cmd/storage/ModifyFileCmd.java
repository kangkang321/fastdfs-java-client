package com.rogrand.fastdfs.client.cmd.storage;

import java.io.FileInputStream;

import com.rogrand.fastdfs.client.cmd.AbstractCmd;
import com.rogrand.fastdfs.client.util.MyException;
import com.rogrand.fastdfs.client.util.ProtoCommon;
import com.rogrand.fastdfs.client.util.ProtoCommon.RecvPackageInfo;

import io.netty.channel.Channel;

public class ModifyFileCmd extends AbstractCmd<Integer> {

	private String groupName;
	private String appenderFilename;
	private long fileOffset;
	private FileInputStream file;

	public ModifyFileCmd(String groupName, String appenderFilename, long fileOffset, FileInputStream file) {
		this.groupName = groupName;
		this.appenderFilename = appenderFilename;
		this.fileOffset = fileOffset;
		this.file = file;
		this.responseCmd = ProtoCommon.STORAGE_PROTO_CMD_RESP;
		this.responseSize = 0;
	}

	@Override
	protected void doRequest(Channel channel) throws Exception {
		byte[] header;
		byte[] hexLenBytes;
		byte[] appenderFilenameBytes;
		int offset;
		long body_len;
		if ((groupName == null || groupName.length() == 0) || (appenderFilename == null || appenderFilename.length() == 0)) {
			throw new MyException("groupName和appenderFilename均不能为空");
		}
		appenderFilenameBytes = appenderFilename.getBytes(ProtoCommon.CHARSET);
		body_len = 3 * ProtoCommon.FDFS_PROTO_PKG_LEN_SIZE + appenderFilenameBytes.length + file.available();

		header = ProtoCommon.packHeader(ProtoCommon.STORAGE_PROTO_CMD_MODIFY_FILE, body_len, (byte) 0);
		byte[] wholePkg = new byte[(int) (header.length + body_len - file.available())];
		System.arraycopy(header, 0, wholePkg, 0, header.length);
		offset = header.length;

		hexLenBytes = ProtoCommon.long2buff(appenderFilename.length());
		System.arraycopy(hexLenBytes, 0, wholePkg, offset, hexLenBytes.length);
		offset += hexLenBytes.length;

		hexLenBytes = ProtoCommon.long2buff(fileOffset);
		System.arraycopy(hexLenBytes, 0, wholePkg, offset, hexLenBytes.length);
		offset += hexLenBytes.length;

		hexLenBytes = ProtoCommon.long2buff(file.available());
		System.arraycopy(hexLenBytes, 0, wholePkg, offset, hexLenBytes.length);
		offset += hexLenBytes.length;

		System.arraycopy(appenderFilenameBytes, 0, wholePkg, offset, appenderFilenameBytes.length);
		offset += appenderFilenameBytes.length;

		channel.write(wholePkg);
		writeFile(channel, file);
		channel.flush();
	}

	@Override
	protected Integer getResponse(RecvPackageInfo response) {
		return (int) response.errno;
	}

}
