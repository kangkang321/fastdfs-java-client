package com.rogrand.fastdfs.client.cmd.tracker;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;

import com.rogrand.fastdfs.client.cmd.AbstractCmd;
import com.rogrand.fastdfs.client.util.ProtoCommon;
import com.rogrand.fastdfs.client.util.ProtoCommon.RecvPackageInfo;

import io.netty.channel.Channel;

public class DeleteStorageCmd extends AbstractCmd<Boolean> {

	private String groupName;

	private String storageIpAddr;

	public DeleteStorageCmd(String groupName, String storageIpAddr) {
		this.groupName = groupName;
		this.storageIpAddr = storageIpAddr;
		this.responseCmd = ProtoCommon.TRACKER_PROTO_CMD_RESP;
		this.responseSize = 0;
	}

	@Override
	protected void doRequest(Channel channel) throws UnsupportedEncodingException {
		byte[] header;
		byte[] bGroupName;
		byte[] bs;
		int len;
		bs = groupName.getBytes(ProtoCommon.CHARSET);
		bGroupName = new byte[ProtoCommon.FDFS_GROUP_NAME_MAX_LEN];

		if (bs.length <= ProtoCommon.FDFS_GROUP_NAME_MAX_LEN) {
			len = bs.length;
		} else {
			len = ProtoCommon.FDFS_GROUP_NAME_MAX_LEN;
		}
		Arrays.fill(bGroupName, (byte) 0);
		System.arraycopy(bs, 0, bGroupName, 0, len);

		int ipAddrLen;
		byte[] bIpAddr = storageIpAddr.getBytes(ProtoCommon.CHARSET);
		if (bIpAddr.length < ProtoCommon.FDFS_IPADDR_SIZE) {
			ipAddrLen = bIpAddr.length;
		} else {
			ipAddrLen = ProtoCommon.FDFS_IPADDR_SIZE - 1;
		}

		header = ProtoCommon.packHeader(ProtoCommon.TRACKER_PROTO_CMD_SERVER_DELETE_STORAGE, ProtoCommon.FDFS_GROUP_NAME_MAX_LEN + ipAddrLen, (byte) 0);
		byte[] wholePkg = new byte[header.length + bGroupName.length + ipAddrLen];
		System.arraycopy(header, 0, wholePkg, 0, header.length);
		System.arraycopy(bGroupName, 0, wholePkg, header.length, bGroupName.length);
		System.arraycopy(bIpAddr, 0, wholePkg, header.length + bGroupName.length, ipAddrLen);
		channel.writeAndFlush(wholePkg);
	}

	@Override
	protected Boolean getResponse(RecvPackageInfo response) {
		return response.errno == 0;
	}

}
