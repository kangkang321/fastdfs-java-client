package com.rogrand.fastdfs.client;

import java.util.List;
import java.util.Map;

import com.rogrand.fastdfs.client.model.StructGroupStat;
import com.rogrand.fastdfs.client.model.StructStorageStat;

public interface TrackerClient {

	public String getUrl(String groupName, String remoteFilename) throws InterruptedException;

	public Map<Integer, String> getUpdateStorages(String groupName, String remoteFilename) throws InterruptedException;

	public Map<Integer, List<String>> getFetchStorages(String groupName, String remoteFilename) throws InterruptedException;

	public StructGroupStat[] listGroups() throws InterruptedException;

	public StructStorageStat[] listStorages(String groupName) throws InterruptedException;

	public boolean deleteStorage(String groupName, String storageIpAddr) throws InterruptedException;

}
