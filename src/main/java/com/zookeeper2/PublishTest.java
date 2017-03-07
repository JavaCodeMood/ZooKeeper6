package com.zookeeper2;

import java.util.concurrent.CountDownLatch;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.RetryNTimes;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggerFactory;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;

/*
 * 发布订阅
 * 一个简单的发布/订阅的实现。集群中每台机器在启动阶段，
 * 都会到该节点上获取数据库的配置信息，
 * 同时客户端还需要在在节点注册一个数据变更的watcher监听，
 * 一旦该数据节点发生变更，就会受到通知信息。
 */
public class PublishTest {

	private static Logger logger = Logger.getLogger(PublishTest.class);
	static CuratorFramework client = null;
	static final String PATH = "/app1/database_config";
	static final String zkAddress = "192.168.90.72:2181";
	static final int timeout = 10000;
	static CountDownLatch countDownLatch = new CountDownLatch(1);
	// 客户端的监听配置
	static ConnectionStateListener clientListener = new ConnectionStateListener() {

		public void stateChanged(CuratorFramework client, ConnectionState newState) {
			if (newState == ConnectionState.CONNECTED) {
				logger.info("connected established");
				countDownLatch.countDown();
			} else if (newState == ConnectionState.LOST) {
				logger.info("connection lost,waiting for reconection");
				try {
					logger.info("reinit---");
					reinit();
					logger.info("inited---");
				} catch (Exception e) {
					logger.error("re-inited failed");
				}
			}

		}
	};

	public static void init() throws Exception {
		client = CuratorFrameworkFactory.builder().connectString(zkAddress).sessionTimeoutMs(timeout)
				.retryPolicy(new RetryNTimes(5, 5000)).build();
		// 客户端注册监听，进行连接配置
		client.getConnectionStateListenable().addListener(clientListener);
		client.start();
		// 连接成功后，才进行下一步的操作
		countDownLatch.await();
	}

	public static void reinit() {
		try {
			unregister();
			init();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void unregister() {
		try {
			if (client != null) {
				client.close();
				client = null;
			}
		} catch (Exception e) {
			logger.info("unregister failed");
		}
	}

	// 对path进行监听配置
	public static String watcherPath(String path, CuratorWatcher watcher) throws Exception {
		byte[] buffer = client.getData().usingWatcher(watcher).forPath(path);
		System.out.println(new String(buffer));
		return new String(buffer);
	}

	public static String readPath(String path) throws Exception {
		byte[] buffer = client.getData().forPath(path);
		return new String(buffer);

	}

	static CuratorWatcher pathWatcher = new CuratorWatcher() {

		public void process(WatchedEvent event) throws Exception {
			// 当数据变化后，重新获取数据信息
			if (event.getType() == EventType.NodeDataChanged) {
				// 获取更改后的数据，进行相应的业务处理
				String value = readPath(event.getPath());
				System.out.println(value);
			}

		}
	};

	public static void main(String[] args) throws Exception {
		init();
		watcherPath(PATH, pathWatcher);
		Thread.sleep(Integer.MAX_VALUE);
	}

}
