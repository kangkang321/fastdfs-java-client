package com.rogrand.fastdfs.client.cmd.storage;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;

import com.rogrand.fastdfs.client.cmd.AbstractCmd;
import com.rogrand.fastdfs.client.model.FileInfo;
import com.rogrand.fastdfs.client.util.ProtoCommon;
import com.rogrand.fastdfs.client.util.ProtoCommon.RecvPackageInfo;

import io.netty.channel.Channel;

public class QueryFileInfoCmd extends AbstractCmd<FileInfo> {

	private String groupName;
	private String remoteFilename;

	public QueryFileInfoCmd(String groupName, String remoteFilename) {
		this.groupName = groupName;
		this.remoteFilename = remoteFilename;
		this.responseCmd = ProtoCommon.STORAGE_PROTO_CMD_RESP;
		this.responseSize = 3 * ProtoCommon.FDFS_PROTO_PKG_LEN_SIZE + ProtoCommon.FDFS_IPADDR_SIZE;
	}

	@Override
	protected void doRequest(Channel channel) throws UnsupportedEncodingException {
		byte[] header;
		byte[] groupBytes;
		byte[] filenameBytes;
		byte[] bs;
		int groupLen;
		filenameBytes = remoteFilename.getBytes(ProtoCommon.CHARSET);
		groupBytes = new byte[ProtoCommon.FDFS_GROUP_NAME_MAX_LEN];
		bs = groupName.getBytes(ProtoCommon.CHARSET);

		Arrays.fill(groupBytes, (byte) 0);
		if (bs.length <= groupBytes.length) {
			groupLen = bs.length;
		} else {
			groupLen = groupBytes.length;
		}
		System.arraycopy(bs, 0, groupBytes, 0, groupLen);

		header = ProtoCommon.packHeader(ProtoCommon.STORAGE_PROTO_CMD_QUERY_FILE_INFO, +groupBytes.length + filenameBytes.length, (byte) 0);
		byte[] wholePkg = new byte[header.length + groupBytes.length + filenameBytes.length];
		System.arraycopy(header, 0, wholePkg, 0, header.length);
		System.arraycopy(groupBytes, 0, wholePkg, header.length, groupBytes.length);
		System.arraycopy(filenameBytes, 0, wholePkg, header.length + groupBytes.length, filenameBytes.length);
		channel.writeAndFlush(wholePkg);
	}

	@Override
	protected FileInfo getResponse(RecvPackageInfo response) {
		long file_size = ProtoCommon.buff2long(response.body, 0);
		int create_timestamp = (int) ProtoCommon.buff2long(response.body, ProtoCommon.FDFS_PROTO_PKG_LEN_SIZE);
		int crc32 = (int) ProtoCommon.buff2long(response.body, 2 * ProtoCommon.FDFS_PROTO_PKG_LEN_SIZE);
		String source_ip_addr = (new String(response.body, 3 * ProtoCommon.FDFS_PROTO_PKG_LEN_SIZE, ProtoCommon.FDFS_IPADDR_SIZE)).trim();
		return new FileInfo(file_size, create_timestamp, crc32, source_ip_addr);
	}

}
