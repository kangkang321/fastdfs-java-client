package com.rogrand.fastdfs.client.cmd.tracker;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;

import com.rogrand.fastdfs.client.cmd.AbstractCmd;
import com.rogrand.fastdfs.client.model.StructStorageStat;
import com.rogrand.fastdfs.client.util.ProtoCommon;
import com.rogrand.fastdfs.client.util.ProtoCommon.RecvPackageInfo;
import com.rogrand.fastdfs.client.util.ProtoStructDecoder;

import io.netty.channel.Channel;

public class ListStoragesCmd extends AbstractCmd<StructStorageStat[]> {

	private String groupName;

	public ListStoragesCmd(String groupName) {
		this.responseCmd = ProtoCommon.TRACKER_PROTO_CMD_RESP;
		this.responseSize = -1;
		this.groupName = groupName;
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

		header = ProtoCommon.packHeader(ProtoCommon.TRACKER_PROTO_CMD_SERVER_LIST_STORAGE, ProtoCommon.FDFS_GROUP_NAME_MAX_LEN, (byte) 0);
		byte[] wholePkg = new byte[header.length + bGroupName.length];
		System.arraycopy(header, 0, wholePkg, 0, header.length);
		System.arraycopy(bGroupName, 0, wholePkg, header.length, bGroupName.length);
		channel.writeAndFlush(wholePkg);

	}

	@Override
	protected StructStorageStat[] getResponse(RecvPackageInfo response) throws Exception {
		if (response.errno != 0) {
			return null;
		}

		ProtoStructDecoder<StructStorageStat> decoder = new ProtoStructDecoder<StructStorageStat>();
		return decoder.decode(response.body, StructStorageStat.class, StructStorageStat.getFieldsTotalSize());
	}

}
