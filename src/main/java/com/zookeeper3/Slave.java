package com.zookeeper3;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;


/**
 * 模拟Zookeeper实现单点故障   主从自动切换
1	创捷父节点类型为Persistent
2	创捷子节点类型为ephemeral + sequential
3	客户端启动时创建子节点
4	序列号最小的子节点选为master，其他子节点都是slave
5	每个slave侦听序列号比它小的子节点中最大的子节点的NodeDeleted事件
6	一旦NodeDeleted事件被触发，该slave客户端会重新选定侦听对象，如果不存在可侦听对象，该slave自动晋升成master
 * http://qindongliang.iteye.com/blog/1985787
 ***/
public class Slave implements Watcher {

	/**
	 * zk实例
	 **/
	public ZooKeeper zk;

	/**
	 * 同步工具
	 * 
	 **/
	private CountDownLatch count = new CountDownLatch(1);

	private static final Charset CHARSET = StandardCharsets.UTF_8;

	public Slave() {
		
	}

	/**
	 * hosts， zookeeper的访问地址
	 * 
	 **/
	public Slave(String hosts) {
		try {
			zk = new ZooKeeper(hosts, 7000, new Watcher() {

				
				public void process(WatchedEvent event) {
					if (event.getState() == Event.KeeperState.SyncConnected) {
						count.countDown();

					}

				}
			});
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/***
	 * 
	 * 此方法是写入数据 如果不存在此节点 就会新建，已存在就是 更新
	 * 
	 **/
	public void write(String path, String value) throws Exception {

		Stat stat = zk.exists(path, false);
		if (stat == null) {
			//创捷父节点类型为Persistent
			zk.create(path, value.getBytes(CHARSET), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		} else {
			zk.setData(path, value.getBytes(CHARSET), -1);
		}

	}

	public String read(String path, Watcher watch) throws Exception {

		byte[] data = zk.getData(path, watch, null);

		return new String(data, CHARSET);
	}

	SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	public void automicSwitch() throws Exception {

		System.out.println("Master故障，Slave自动切换.......,  时间  " + f.format(new Date()));

	}

	public void startMaster() {

		System.out.println("A的Master 启动了........");
	}

	public void createPersist() throws Exception {

		zk.create("/a", "主节点".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

		System.out.println("创建主节点成功........");

	}

	public void createTemp() throws Exception {
        //创捷子节点类型为ephemeral + sequential
		zk.create("/a/b", "a".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

		System.out.println("a创建子节点成功...........");

	}

	public void check() throws Exception {
		List<String> list = zk.getChildren("/a", null);
		Collections.sort(list);
		if (list.isEmpty()) {
			System.out.println("此父路径下面没有节点");
		} else {

			String start = list.get(0);

			String data = new String(zk.getData("/a/" + start, false, null));
			if (data.equals("a")) {// 等于本身就启动作为Master

				if (list.size() == 1) {
					startMaster();// 作为Master启动
				} else {
					automicSwitch();
				}
			} else {
				// 非当前节点
				for (int i = 0; i < list.size(); i++) {
					// 获取那个节点存的此客户端的模拟IP
					String temp = new String(zk.getData("/a/" + list.get(i), false, null));

					if (temp.equals("a")) {
						// 因为前面作为首位判断，所以这个出现的位置不可能是首位
						// 需要监听小节点里面的最大的一个节点
						String watchPath = list.get(i - 1);
						System.out.println("a监听的是:  " + watchPath);

						zk.exists("/a/" + watchPath, this);// 监听此节点的详细情况
						break;// 结束循环
					}

				}

			}

		}

	}

	public void close() throws Exception {
		zk.close();
	}

	
	public void process(WatchedEvent event) {

		if (event.getType() == Event.EventType.NodeDeleted) {

			// 如果发现，监听的节点，挂掉了，那么就重新，进行监听
			try {
				System.out.println("注意有节点挂掉，重新调整监听策略........");
				check();
			} catch (Exception e) {
				e.printStackTrace();

			}
		}

	}

	public static void main(String[] args) throws Exception {

		Slave s = new Slave("10.59.0.40:2181");
		// s.createPersist();//创建主节点
		s.createTemp();
		s.check();
		Thread.sleep(Long.MAX_VALUE);
		s.close();

	}

}