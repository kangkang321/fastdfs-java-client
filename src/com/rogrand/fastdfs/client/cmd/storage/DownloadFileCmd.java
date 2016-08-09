package com.rogrand.fastdfs.client.cmd.storage;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;

import com.rogrand.fastdfs.client.cmd.AbstractCmd;
import com.rogrand.fastdfs.client.util.ProtoCommon;
import com.rogrand.fastdfs.client.util.ProtoCommon.RecvPackageInfo;

import io.netty.channel.Channel;

public class DownloadFileCmd extends AbstractCmd<byte[]> {

	private String groupName;
	private String remoteFilename;
	private long fileOffset;
	private long downloadBytes;

	public DownloadFileCmd(String groupName, String remoteFilename, long fileOffset, long downloadBytes) {
		this.groupName = groupName;
		this.remoteFilename = remoteFilename;
		this.fileOffset = fileOffset;
		this.downloadBytes = downloadBytes;
		this.responseCmd = ProtoCommon.STORAGE_PROTO_CMD_RESP;
		this.responseSize = -1;
	}

	@Override
	protected void doRequest(Channel channel) throws UnsupportedEncodingException {
		byte[] header;
		byte[] bsOffset;
		byte[] bsDownBytes;
		byte[] groupBytes;
		byte[] filenameBytes;
		byte[] bs;
		int groupLen;

		bsOffset = ProtoCommon.long2buff(fileOffset);
		bsDownBytes = ProtoCommon.long2buff(downloadBytes);
		groupBytes = new byte[ProtoCommon.FDFS_GROUP_NAME_MAX_LEN];
		bs = groupName.getBytes(ProtoCommon.CHARSET);
		filenameBytes = remoteFilename.getBytes(ProtoCommon.CHARSET);

		Arrays.fill(groupBytes, (byte) 0);
		if (bs.length <= groupBytes.length) {
			groupLen = bs.length;
		} else {
			groupLen = groupBytes.length;
		}
		System.arraycopy(bs, 0, groupBytes, 0, groupLen);

		header = ProtoCommon.packHeader(ProtoCommon.STORAGE_PROTO_CMD_DOWNLOAD_FILE,
										bsOffset.length + bsDownBytes.length + groupBytes.length + filenameBytes.length,
										(byte) 0);
		byte[] wholePkg = new byte[header.length + bsOffset.length + bsDownBytes.length + groupBytes.length + filenameBytes.length];
		System.arraycopy(header, 0, wholePkg, 0, header.length);
		System.arraycopy(bsOffset, 0, wholePkg, header.length, bsOffset.length);
		System.arraycopy(bsDownBytes, 0, wholePkg, header.length + bsOffset.length, bsDownBytes.length);
		System.arraycopy(groupBytes, 0, wholePkg, header.length + bsOffset.length + bsDownBytes.length, groupBytes.length);
		System.arraycopy(filenameBytes, 0, wholePkg, header.length + bsOffset.length + bsDownBytes.length + groupBytes.length, filenameBytes.length);
		channel.writeAndFlush(wholePkg);
	}

	@Override
	protected byte[] getResponse(RecvPackageInfo response) {
		return response.body;
	}

}
