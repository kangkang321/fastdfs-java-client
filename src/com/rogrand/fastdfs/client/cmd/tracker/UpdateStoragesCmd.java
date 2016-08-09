package com.rogrand.fastdfs.client.cmd.tracker;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

import com.rogrand.fastdfs.client.cmd.AbstractCmd;
import com.rogrand.fastdfs.client.util.MyException;
import com.rogrand.fastdfs.client.util.ProtoCommon;
import com.rogrand.fastdfs.client.util.ProtoCommon.RecvPackageInfo;

import io.netty.channel.Channel;

public class UpdateStoragesCmd extends AbstractCmd<Map<Integer, String>> {

	private String groupName;
	private String remoteFilename;

	public UpdateStoragesCmd(String groupName, String remoteFilename) {
		this.groupName = groupName;
		this.remoteFilename = remoteFilename;
		this.responseCmd = ProtoCommon.TRACKER_PROTO_CMD_RESP;
		this.responseSize = -1;
	}

	@Override
	protected void doRequest(Channel channel) throws UnsupportedEncodingException {
		channel.writeAndFlush(getRequest(ProtoCommon.TRACKER_PROTO_CMD_SERVICE_QUERY_UPDATE, groupName, remoteFilename));
	}

	@Override
	protected Map<Integer, String> getResponse(RecvPackageInfo response) {
		String ip_addr;
		int port;
		if (response.body.length < ProtoCommon.TRACKER_QUERY_STORAGE_FETCH_BODY_LEN) {
			throw new MyException("Invalid body length: " + response.body.length);
		}

		if ((response.body.length - ProtoCommon.TRACKER_QUERY_STORAGE_FETCH_BODY_LEN) % (ProtoCommon.FDFS_IPADDR_SIZE - 1) != 0) {
			throw new MyException("Invalid body length: " + response.body.length);
		}

		ip_addr = new String(response.body, ProtoCommon.FDFS_GROUP_NAME_MAX_LEN, ProtoCommon.FDFS_IPADDR_SIZE - 1).trim();
		int offset = ProtoCommon.FDFS_GROUP_NAME_MAX_LEN + ProtoCommon.FDFS_IPADDR_SIZE - 1;

		port = (int) ProtoCommon.buff2long(response.body, offset);
		Map<Integer, String> result = new HashMap<Integer, String>();
		result.put(port, ip_addr);
		return result;
	}

}
