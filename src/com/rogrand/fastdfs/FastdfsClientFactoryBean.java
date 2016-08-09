package com.rogrand.fastdfs;

import java.io.FileInputStream;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

import com.rogrand.fastdfs.client.StorageClient;
import com.rogrand.fastdfs.client.TrackerClient;
import com.rogrand.fastdfs.client.cmd.AbstractCmd.FastdfsChannelHandler;
import com.rogrand.fastdfs.client.impl.StorageClientImpl;
import com.rogrand.fastdfs.client.impl.TrackerClientImpl;
import com.rogrand.fastdfs.client.model.StructGroupStat;
import com.rogrand.fastdfs.client.model.StructStorageStat;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.pool.AbstractChannelPoolHandler;
import io.netty.channel.pool.ChannelPool;
import io.netty.channel.pool.FixedChannelPool;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.bytes.ByteArrayDecoder;
import io.netty.handler.codec.bytes.ByteArrayEncoder;

/**
 * 后续与spring结合，改造成BeanFactory
 * 
 * @author admins(admins@rogrand.com)
 * @version 2016年8月9日 上午11:23:22
 */
public class FastdfsClientFactoryBean implements FastdfsClient {

	private Map<String, Integer> trackers;

	private Map<String, ChannelPool> trackerPool;

	private Map<String, Map<String, ChannelPool>> storagePool;

	/**
	 * FIXME 这里返回第一个，后续考虑负载均衡的问题
	 * 
	 * @return
	 */
	protected TrackerClient getTracker() {
		for (ChannelPool pool : trackerPool.values()) {
			return new TrackerClientImpl(pool);
		}
		return null;
	}

	/**
	 * FIMXE 这里返回第一个，后续考虑负载均衡的问题
	 * 
	 * @param groupName
	 * @return
	 */
	protected StorageClient getStorage(String groupName) {
		if (groupName == null) {
			for (Map<String, ChannelPool> pools : storagePool.values()) {
				for (ChannelPool pool : pools.values()) {
					return new StorageClientImpl(pool);
				}
			}
		} else {
			for (ChannelPool pool : storagePool.get(groupName).values()) {
				return new StorageClientImpl(pool);
			}
		}
		return null;
	}

	private void init() {
		// 初始化tracker
		trackerPool = new HashMap<String, ChannelPool>();
		for (String key : trackers.keySet()) {
			trackerPool.put(key, createChannelPool(key, trackers.get(key)));
		}
		// 初始化storage
		TrackerClient tracker = getTracker();
		try {
			storagePool = new HashMap<String, Map<String, ChannelPool>>();
			StructGroupStat[] groups = tracker.listGroups();
			for (StructGroupStat group : groups) {
				Map<String, ChannelPool> pools = storagePool.get(group.getGroupName());
				if (pools == null) {
					pools = new HashMap<String, ChannelPool>();
					storagePool.put(group.getGroupName(), pools);
				}
				StructStorageStat[] storages = tracker.listStorages(group.getGroupName());
				for (StructStorageStat storage : storages) {
					pools.put(storage.getIpAddr(), createChannelPool(storage.getIpAddr(), storage.getStoragePort()));
				}
			}
		}
		catch (InterruptedException e) {}
	}

	private ChannelPool createChannelPool(String address, Integer port) {
		EventLoopGroup group = new NioEventLoopGroup();
		Bootstrap b = new Bootstrap();
		b.group(group).remoteAddress(new InetSocketAddress(address, 23000)).channel(NioSocketChannel.class).option(ChannelOption.TCP_NODELAY, true);
		FixedChannelPool pool = new FixedChannelPool(b, new AbstractChannelPoolHandler() {

			public void channelCreated(Channel ch) throws Exception {
				ChannelPipeline p = ch.pipeline();
				p.addLast(new ByteArrayDecoder()).addLast(new ByteArrayEncoder()).addLast(new FastdfsChannelHandler());
			}
		}, 5);
		return pool;
	}

	public String[] upload_file(FileInputStream file, String fileExtName) throws InterruptedException {
		return getStorage(null).upload_file(file, fileExtName);
	}

	public String[] upload_appender_file(FileInputStream file, String fileExtName) throws InterruptedException {
		return getStorage(null).upload_file(file, fileExtName);
	}

	public String[] upload_slave_file(FileInputStream file, String masterFileId, String prefixName, String fileExtName) throws InterruptedException {
		return getStorage(masterFileId.split("/")[0]).upload_slave_file(file, masterFileId, prefixName, fileExtName);
	}

	public int append_file(String groupName, String appenderFilename, FileInputStream file) throws InterruptedException {
		return getStorage(groupName).append_file(groupName, appenderFilename, file);
	}

	public int modify_file(String groupName, String appenderFilename, long fileOffset, FileInputStream file) throws InterruptedException {
		return getStorage(groupName).modify_file(groupName, appenderFilename, fileOffset, file);
	}

	public int delete_file(String groupName, String remoteFilename) throws InterruptedException {
		return getStorage(groupName).delete_file(groupName, remoteFilename);
	}

	public String getUrl(String groupName, String remoteFilename) throws InterruptedException {
		return getTracker().getUrl(groupName, remoteFilename);
	}
}
