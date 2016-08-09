package com.rogrand.fastdfs.client.cmd.storage;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;

import com.rogrand.fastdfs.client.cmd.AbstractCmd;
import com.rogrand.fastdfs.client.model.NameValuePair;
import com.rogrand.fastdfs.client.util.ProtoCommon;
import com.rogrand.fastdfs.client.util.ProtoCommon.RecvPackageInfo;

import io.netty.channel.Channel;

public class SetMetadataCmd extends AbstractCmd<Integer> {

	private String groupName;

	private String remoteFilename;

	private NameValuePair[] metaList;

	private byte opFlag;

	public SetMetadataCmd(String groupName, String remoteFilename, NameValuePair[] metaList, byte opFlag) {
		this.groupName = groupName;
		this.remoteFilename = remoteFilename;
		this.metaList = metaList;
		this.opFlag = opFlag;
		this.responseCmd = ProtoCommon.STORAGE_PROTO_CMD_RESP;
		this.responseSize = 0;

	}

	@Override
	protected void doRequest(Channel channel) throws UnsupportedEncodingException {
		byte[] header;
		byte[] groupBytes;
		byte[] filenameBytes;
		byte[] meta_buff;
		byte[] bs;
		int groupLen;
		byte[] sizeBytes;
		if (metaList == null) {
			meta_buff = new byte[0];
		} else {
			meta_buff = ProtoCommon.pack_metadata(metaList).getBytes(ProtoCommon.CHARSET);
		}

		filenameBytes = remoteFilename.getBytes(ProtoCommon.CHARSET);
		sizeBytes = new byte[2 * ProtoCommon.FDFS_PROTO_PKG_LEN_SIZE];
		Arrays.fill(sizeBytes, (byte) 0);

		bs = ProtoCommon.long2buff(filenameBytes.length);
		System.arraycopy(bs, 0, sizeBytes, 0, bs.length);
		bs = ProtoCommon.long2buff(meta_buff.length);
		System.arraycopy(bs, 0, sizeBytes, ProtoCommon.FDFS_PROTO_PKG_LEN_SIZE, bs.length);

		groupBytes = new byte[ProtoCommon.FDFS_GROUP_NAME_MAX_LEN];
		bs = groupName.getBytes(ProtoCommon.CHARSET);

		Arrays.fill(groupBytes, (byte) 0);
		if (bs.length <= groupBytes.length) {
			groupLen = bs.length;
		} else {
			groupLen = groupBytes.length;
		}
		System.arraycopy(bs, 0, groupBytes, 0, groupLen);

		header = ProtoCommon.packHeader(ProtoCommon.STORAGE_PROTO_CMD_SET_METADATA,
										2 * ProtoCommon.FDFS_PROTO_PKG_LEN_SIZE + 1 + groupBytes.length + filenameBytes.length + meta_buff.length,
										(byte) 0);
		byte[] wholePkg = new byte[header.length + sizeBytes.length + 1 + groupBytes.length + filenameBytes.length];
		System.arraycopy(header, 0, wholePkg, 0, header.length);
		System.arraycopy(sizeBytes, 0, wholePkg, header.length, sizeBytes.length);
		wholePkg[header.length + sizeBytes.length] = opFlag;
		System.arraycopy(groupBytes, 0, wholePkg, header.length + sizeBytes.length + 1, groupBytes.length);
		System.arraycopy(filenameBytes, 0, wholePkg, header.length + sizeBytes.length + 1 + groupBytes.length, filenameBytes.length);
		channel.write(wholePkg);
		if (meta_buff.length > 0) {
			channel.write(meta_buff);
		}
		channel.flush();
	}

	@Override
	protected Integer getResponse(RecvPackageInfo response) {
		return (int) response.errno;
	}

}
