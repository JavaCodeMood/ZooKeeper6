package com.zookeeper;

import java.io.IOException;

import com.github.zkclient.IZkDataListener;
import com.github.zkclient.ZkClient;

public class ZkClientTest {

    public static void main(String[] args) {
        ZkClient zkClient = new ZkClient("127.0.0.1:2181");
        zkClient.subscribeDataChanges("/db", new IZkDataListener() {
            public void handleDataChange(String dataPath, byte[] data) throws Exception {
                System.out.println(new String(data));
            }

            public void handleDataDeleted(String dataPath) throws Exception {
                System.out.println(dataPath);
            }
        });

        try {
            System.in.read();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}