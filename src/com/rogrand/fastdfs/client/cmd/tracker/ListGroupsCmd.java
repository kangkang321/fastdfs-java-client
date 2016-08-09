package com.rogrand.fastdfs.client.cmd.tracker;

import java.io.UnsupportedEncodingException;

import com.rogrand.fastdfs.client.cmd.AbstractCmd;
import com.rogrand.fastdfs.client.model.StructGroupStat;
import com.rogrand.fastdfs.client.util.ProtoCommon;
import com.rogrand.fastdfs.client.util.ProtoCommon.RecvPackageInfo;
import com.rogrand.fastdfs.client.util.ProtoStructDecoder;

import io.netty.channel.Channel;

public class ListGroupsCmd extends AbstractCmd<StructGroupStat[]> {

	public ListGroupsCmd() {
		this.responseCmd = ProtoCommon.TRACKER_PROTO_CMD_RESP;
		this.responseSize = -1;
	}

	@Override
	protected void doRequest(Channel channel) throws UnsupportedEncodingException {
		channel.writeAndFlush(ProtoCommon.packHeader(ProtoCommon.TRACKER_PROTO_CMD_SERVER_LIST_GROUP, 0, (byte) 0));
	}

	@Override
	protected StructGroupStat[] getResponse(RecvPackageInfo response) throws Exception {
		ProtoStructDecoder<StructGroupStat> decoder = new ProtoStructDecoder<StructGroupStat>();
		return decoder.decode(response.body, StructGroupStat.class, StructGroupStat.getFieldsTotalSize());
	}

}
