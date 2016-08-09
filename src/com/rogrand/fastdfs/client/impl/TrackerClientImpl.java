package com.rogrand.fastdfs.client.impl;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

import com.rogrand.fastdfs.client.TrackerClient;
import com.rogrand.fastdfs.client.cmd.AbstractCmd;
import com.rogrand.fastdfs.client.cmd.tracker.DeleteStorageCmd;
import com.rogrand.fastdfs.client.cmd.tracker.FetchStoragesCmd;
import com.rogrand.fastdfs.client.cmd.tracker.ListGroupsCmd;
import com.rogrand.fastdfs.client.cmd.tracker.ListStoragesCmd;
import com.rogrand.fastdfs.client.cmd.tracker.UpdateStoragesCmd;
import com.rogrand.fastdfs.client.model.StructGroupStat;
import com.rogrand.fastdfs.client.model.StructStorageStat;
import com.rogrand.fastdfs.client.util.MyException;

import io.netty.channel.Channel;
import io.netty.channel.pool.ChannelPool;
import io.netty.util.concurrent.Future;

public class TrackerClientImpl implements TrackerClient {

	private final ChannelPool channelPool;

	public TrackerClientImpl(ChannelPool channelPool) {
		this.channelPool = channelPool;
	}

	public Map<Integer, String> getUpdateStorages(String groupName, String remoteFilename) throws InterruptedException {
		return execCommon(new UpdateStoragesCmd(groupName, remoteFilename));
	}

	public Map<Integer, List<String>> getFetchStorages(String groupName, String remoteFilename) throws InterruptedException {
		return execCommon(new FetchStoragesCmd(groupName, remoteFilename));
	}

	public StructGroupStat[] listGroups() throws InterruptedException {
		return execCommon(new ListGroupsCmd());
	}

	public StructStorageStat[] listStorages(String groupName) throws InterruptedException {
		return execCommon(new ListStoragesCmd(groupName));
	}

	public boolean deleteStorage(String groupName, String storageIpAddr) throws InterruptedException {
		return execCommon(new DeleteStorageCmd(groupName, storageIpAddr));
	}

	private <T> T execCommon(AbstractCmd<T> cmd) throws InterruptedException {
		Future<Channel> future = channelPool.acquire();
		future.sync();
		Channel channel;
		try {
			channel = future.get();
		}
		catch (ExecutionException e) {
			throw new MyException("未取得有效连接", e);
		}
		T result = cmd.execute(channel);
		channelPool.release(channel).sync();
		return result;
	}

	public String getUrl(String groupName, String remoteFilename) throws InterruptedException {
		final StringBuffer sb = new StringBuffer();
		final StructStorageStat[] stats = listStorages(groupName);
		Map<Integer, String> map = getUpdateStorages(groupName, remoteFilename);
		map.values().forEach(new Consumer<String>() {

			public void accept(String t) {
				for (StructStorageStat s : stats) {
					if (t.equals(s.getIpAddr())) {
						sb.append("http://");
						sb.append(s.getDomainName());
						sb.append(":");
						sb.append(s.getStorageHttpPort());
					}
				}
			}

		});
		sb.append("/");
		sb.append(groupName);
		sb.append("/");
		sb.append(remoteFilename);
		return sb.toString();
	}

}
