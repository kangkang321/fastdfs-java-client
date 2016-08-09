package com.rogrand.fastdfs.client.cmd.storage;

import java.io.FileInputStream;
import java.util.Arrays;

import com.rogrand.fastdfs.client.cmd.AbstractCmd;
import com.rogrand.fastdfs.client.util.MyException;
import com.rogrand.fastdfs.client.util.ProtoCommon;
import com.rogrand.fastdfs.client.util.ProtoCommon.RecvPackageInfo;

import io.netty.channel.Channel;

/**
 * 普通文件上传
 * 
 * @author admins(admins@rogrand.com)
 * @version 2016年8月9日 上午10:41:15
 */
public class UploadFileCmd extends AbstractCmd<String[]> {

	private FileInputStream file;
	private String fileExtName;

	public UploadFileCmd(FileInputStream file, String fileExtName) {
		this.file = file;
		this.fileExtName = fileExtName;
		this.responseCmd = ProtoCommon.STORAGE_PROTO_CMD_RESP;
		this.responseSize = -1;
	}

	@Override
	protected void doRequest(Channel channel) throws Exception {
		byte[] header;
		byte[] ext_name_bs;
		byte[] sizeBytes;
		byte[] hexLenBytes;
		int offset;
		long body_len;
		ext_name_bs = new byte[ProtoCommon.FDFS_FILE_EXT_NAME_MAX_LEN];
		Arrays.fill(ext_name_bs, (byte) 0);
		if (fileExtName != null && fileExtName.length() > 0) {
			byte[] bs = fileExtName.getBytes(ProtoCommon.CHARSET);
			int ext_name_len = bs.length;
			if (ext_name_len > ProtoCommon.FDFS_FILE_EXT_NAME_MAX_LEN) {
				ext_name_len = ProtoCommon.FDFS_FILE_EXT_NAME_MAX_LEN;
			}
			System.arraycopy(bs, 0, ext_name_bs, 0, ext_name_len);
		}

		sizeBytes = new byte[1 + 1 * ProtoCommon.FDFS_PROTO_PKG_LEN_SIZE];
		body_len = sizeBytes.length + ProtoCommon.FDFS_FILE_EXT_NAME_MAX_LEN + file.available();
		// FIXME 这里后期调整下
		sizeBytes[0] = (byte) 0;
		offset = 1;

		hexLenBytes = ProtoCommon.long2buff(file.available());
		System.arraycopy(hexLenBytes, 0, sizeBytes, offset, hexLenBytes.length);

		header = ProtoCommon.packHeader(ProtoCommon.STORAGE_PROTO_CMD_UPLOAD_FILE, body_len, (byte) 0);
		byte[] wholePkg = new byte[(int) (header.length + body_len - file.available())];
		System.arraycopy(header, 0, wholePkg, 0, header.length);
		System.arraycopy(sizeBytes, 0, wholePkg, header.length, sizeBytes.length);
		offset = header.length + sizeBytes.length;

		System.arraycopy(ext_name_bs, 0, wholePkg, offset, ext_name_bs.length);
		offset += ext_name_bs.length;

		channel.write(wholePkg);
		writeFile(channel, file);
		channel.flush();
	}

	@Override
	protected String[] getResponse(RecvPackageInfo response) {
		String new_group_name;
		String remote_filename;
		if (response.body.length <= ProtoCommon.FDFS_GROUP_NAME_MAX_LEN) {
			throw new MyException("body length: " + response.body.length + " <= " + ProtoCommon.FDFS_GROUP_NAME_MAX_LEN);
		}

		new_group_name = new String(response.body, 0, ProtoCommon.FDFS_GROUP_NAME_MAX_LEN).trim();
		remote_filename = new String(response.body, ProtoCommon.FDFS_GROUP_NAME_MAX_LEN, response.body.length - ProtoCommon.FDFS_GROUP_NAME_MAX_LEN);
		String[] results = new String[2];
		results[0] = new_group_name;
		results[1] = remote_filename;

		return results;
	}

}
