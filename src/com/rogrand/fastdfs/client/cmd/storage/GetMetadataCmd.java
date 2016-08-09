package com.rogrand.fastdfs.client.cmd.storage;

import java.io.UnsupportedEncodingException;

import com.rogrand.fastdfs.client.cmd.AbstractCmd;
import com.rogrand.fastdfs.client.model.NameValuePair;
import com.rogrand.fastdfs.client.util.ProtoCommon;
import com.rogrand.fastdfs.client.util.ProtoCommon.RecvPackageInfo;

import io.netty.channel.Channel;

public class GetMetadataCmd extends AbstractCmd<NameValuePair[]> {

	private String groupName;
	private String remoteFilename;

	public GetMetadataCmd(String groupName, String remoteFilename) {
		this.groupName = groupName;
		this.remoteFilename = remoteFilename;
		this.responseCmd = ProtoCommon.STORAGE_PROTO_CMD_RESP;
		this.responseSize = -1;
	}

	@Override
	protected void doRequest(Channel channel) throws UnsupportedEncodingException {
		channel.writeAndFlush(getRequest(ProtoCommon.STORAGE_PROTO_CMD_GET_METADATA, groupName, remoteFilename));
	}

	@Override
	protected NameValuePair[] getResponse(RecvPackageInfo response) throws UnsupportedEncodingException {
		return ProtoCommon.split_metadata(new String(response.body, ProtoCommon.CHARSET));
	}

}
