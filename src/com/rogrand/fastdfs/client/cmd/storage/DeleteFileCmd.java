package com.rogrand.fastdfs.client.cmd.storage;

import java.io.UnsupportedEncodingException;

import com.rogrand.fastdfs.client.cmd.AbstractCmd;
import com.rogrand.fastdfs.client.util.ProtoCommon;
import com.rogrand.fastdfs.client.util.ProtoCommon.RecvPackageInfo;

import io.netty.channel.Channel;

public class DeleteFileCmd extends AbstractCmd<Integer> {

	private String groupName;
	private String remoteFilename;

	public DeleteFileCmd(String groupName, String remoteFilename) {
		this.groupName = groupName;
		this.remoteFilename = remoteFilename;
		this.responseCmd = ProtoCommon.STORAGE_PROTO_CMD_RESP;
		this.responseSize = 0;
	}

	@Override
	protected void doRequest(Channel channel) throws UnsupportedEncodingException {
		channel.writeAndFlush(getRequest(ProtoCommon.STORAGE_PROTO_CMD_DELETE_FILE, groupName, remoteFilename));
	}

	@Override
	protected Integer getResponse(RecvPackageInfo response) {
		return (int) response.errno;
	}

}
