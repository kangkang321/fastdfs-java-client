package com.rogrand.fastdfs.client.cmd;

import java.io.FileInputStream;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.concurrent.SynchronousQueue;

import com.rogrand.fastdfs.client.util.MyException;
import com.rogrand.fastdfs.client.util.ProtoCommon;
import com.rogrand.fastdfs.client.util.ProtoCommon.RecvPackageInfo;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.AttributeKey;

public abstract class AbstractCmd<T> {

	public static final AttributeKey<Byte> RESPONSECMD = AttributeKey.valueOf("responseCmd");
	public static final AttributeKey<Long> RESPONSESIZE = AttributeKey.valueOf("responseSize");
	public static final AttributeKey<SynchronousQueue<RecvPackageInfo>> RESPONSE = AttributeKey.valueOf("response");
	public static final AttributeKey<RecvPackageInfo> RECV = AttributeKey.valueOf("recv");
	protected byte responseCmd;
	protected long responseSize;

	public T execute(Channel channel) throws InterruptedException {
		channel.attr(RESPONSECMD).set(responseCmd);
		channel.attr(RESPONSESIZE).set(responseSize);
		channel.attr(RESPONSE).set(new SynchronousQueue<RecvPackageInfo>());
		try {
			// 请求
			doRequest(channel);
			// 返回值
			return getResponse(channel.attr(RESPONSE).get().take());
		}
		catch (Exception e) {
			throw new MyException("命令执行失败", e);
		}
	}

	protected abstract void doRequest(Channel channel) throws Exception;

	protected abstract T getResponse(RecvPackageInfo response) throws Exception;

	protected byte[] getRequest(byte cmd, String groupName, String remoteFilename) throws UnsupportedEncodingException {
		byte[] header;
		byte[] groupBytes;
		byte[] filenameBytes;
		byte[] bs;
		int groupLen;

		groupBytes = new byte[ProtoCommon.FDFS_GROUP_NAME_MAX_LEN];
		bs = groupName.getBytes(ProtoCommon.CHARSET);
		filenameBytes = remoteFilename.getBytes(ProtoCommon.CHARSET);

		Arrays.fill(groupBytes, (byte) 0);
		if (bs.length <= groupBytes.length) {
			groupLen = bs.length;
		} else {
			groupLen = groupBytes.length;
		}
		System.arraycopy(bs, 0, groupBytes, 0, groupLen);

		header = ProtoCommon.packHeader(cmd, groupBytes.length + filenameBytes.length, (byte) 0);
		byte[] wholePkg = new byte[header.length + groupBytes.length + filenameBytes.length];
		System.arraycopy(header, 0, wholePkg, 0, header.length);
		System.arraycopy(groupBytes, 0, wholePkg, header.length, groupBytes.length);
		System.arraycopy(filenameBytes, 0, wholePkg, header.length + groupBytes.length, filenameBytes.length);
		return wholePkg;
	}

	protected void writeFile(Channel channel, FileInputStream file) throws Exception {
		byte[] bytes = new byte[1024 * 1024];
		int read;
		while ((read = file.read(bytes)) >= 0) {
			channel.write(Arrays.copyOf(bytes, read));
		}
	}

	public static class FastdfsChannelHandler extends ChannelInboundHandlerAdapter {

		@SuppressWarnings("deprecation")
		@Override
		public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
			byte responseCmd = ctx.channel().attr(RESPONSECMD).get();
			long responseSize = ctx.channel().attr(RESPONSESIZE).get();
			SynchronousQueue<RecvPackageInfo> response = ctx.channel().attr(RESPONSE).get();
			byte[] bytes = (byte[]) msg;
			RecvPackageInfo recv = ProtoCommon.recvPackage(bytes, responseCmd, responseSize, ctx.attr(RECV).get());
			ctx.attr(RECV).set(recv);
			if (recv.errno != (byte) -1) {
				response.put(recv);
			}
		}

		@SuppressWarnings("deprecation")
		@Override
		public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
			RecvPackageInfo recv = ctx.attr(RECV).get();
			ctx.attr(RECV).set(null);
			if (null != recv && recv.errno == (byte) -1) {
				throw new MyException("解析结果失败！");
			}
		}

	}
}
