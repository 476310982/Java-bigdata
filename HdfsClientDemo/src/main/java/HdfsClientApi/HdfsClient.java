package com.atguigu.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsClient {
    /*客户端操作*/
    @Test
    public void testMkdirs() throws URISyntaxException, IOException, InterruptedException {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://hadoop102:9000");
        // 配置在集群上运行
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "atguigu");
        // 2 创建目录
        fs.mkdirs(new Path("/hadoop/HdfsClientAPI"));
        // 3 关闭资源
        fs.close();
        System.out.println("over!");
    }

    /*
    * 优化2
    */
    @Test
    public void getClient() throws Exception {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(
                new URI("hdfs://hadoop102:9000"),
                configuration,
                "atguigu");
        fileSystem.mkdirs(new Path("/hadoop/HdfsClientAPI2"));
        fileSystem.close();
        System.out.println("创建目录成功！");

    }

    /*测试文件上传*/
    @Test
    public void testPutFile() throws Exception {
        /*获得hdfs的客户端*/
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "atguigu");
        /*上传文件*/
        fs.copyFromLocalFile(new Path("C:Users/Maxwell/Desktop/HdfsClientAPIFile.txt"), new Path("/HdfsClientAPIFile.txt"));
        /*关闭资源*/
        fs.close();
        System.out.println("文件上传成功！");
    }

    /*测试文件下载*/
    @Test
    public void testDownload() throws Exception {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "atguigu");
        fs.copyToLocalFile(true, new Path("/HdfsClientAPIFile.txt"), new Path("C:Users/Maxwell/Desktop/HdfsClientAPIFile.txt"), true);
        fs.close();
        System.out.println("文件下载完成！");
    }


    /*测试文件重命名*/
    @Test
    public void testRename() throws Exception {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "atguigu");
        fs.rename(new Path("/HdfsClientAPIFile.txt"), new Path("/HdfsClientAPIFile2.txt"));
        fs.close();
        System.out.println("文件重命名成功完成！");
    }

    /*查看文件详情，返回迭代器*/
    @Test
    public void testListFile() throws Exception {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "atguigu");
        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(new Path("/"), false);
        System.out.println(iterator);
        while (iterator.hasNext()) {
            LocatedFileStatus status = iterator.next();
            System.out.println(status.getLen());
            System.out.println(status.getPermission());
            System.out.println(status.getGroup());
            System.out.println("获取块的信息:");
            BlockLocation[] blockLocations = status.getBlockLocations();
            System.out.println(blockLocations.length);
            for (BlockLocation blockLocation : blockLocations) {
                String[] hosts = blockLocation.getHosts();
                for (String host : hosts) {
                    System.out.println(host);
                }
            }
            System.out.println("===================================================================");
        }
        fs.close();
        System.out.println("Over!");
    }

    /*判断文件夹或者文件*/
    @Test
    public void testIsFile() throws Exception {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "atguigu");
        Path[] paths = {new Path("/"), new Path("/hbase")};
//        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
        FileStatus[] fileStatuses = fs.listStatus(paths);
        for (FileStatus fileStatus : fileStatuses) {
            if (fileStatus.isFile()) {
                System.out.println("File:" + fileStatus.getPath());
//                System.out.println("File:" + fileStatus.getPath().getName());
            } else {
                System.out.println("Diretory:" + fileStatus.getPath());
            }
        }
        fs.close();
    }

    /*HDFS文件上传：数据流*/
    @Test
    public void testIOPutFile() throws Exception {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "atguigu");
        FileInputStream fis = new FileInputStream(new File("C:Users/Maxwell/Desktop/hadoop-2.7.2.rar"));
        FSDataOutputStream fsdos = fs.create(new Path("/hadoop-2.7.2.rar"), true);
        /*拷贝流*/
//        IOUtils.copyBytes(fis,fsdos,configuration,true);/*close:true 自动关闭流*/
        IOUtils.copyBytes(fis, fsdos, configuration);
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fsdos);
    }

    /*HDFS 文件下载*/
    @Test
    public void testIODownLoad() throws Exception {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "atguigu");
        FSDataInputStream fsdis = fs.open(new Path("/HdfsClientAPIFileIO.txt"));
        FileOutputStream fos = new FileOutputStream("C:Users/Maxwell/Desktop/HdfsClientAPIFileIO.txt");
        IOUtils.copyBytes(fsdis, fos, configuration, true);
        System.out.println("HDFS文件下载完成！");
    }

    /*定位文件读取*/
    @Test
    public void testSeekBlock1() throws Exception {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "atguigu");
        FSDataInputStream fsdis = fs.open(new Path("/hadoop-2.7.2.rar"));
        FileOutputStream fos = new FileOutputStream("C:Users/Maxwell/Desktop/hadoop-2.7.2.zip");
        byte[] buf = new byte[1024];
        for (int i = 0; i < 1024 * 128; i++) {
            fsdis.read(buf);
            fos.write(buf);
        }
        fsdis.close();
        fos.close();
    }

    @Test
    public void testSeekBlock2() throws Exception {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "atguigu");
        FSDataInputStream fsdis = fs.open(new Path("/hadoop-2.7.2.rar"));
        FileOutputStream fos = new FileOutputStream("C:Users/Maxwell/Desktop/hadoop-2.7.2_1.zip");
        /*关键步骤：定位第二个Block的位置*/
        fsdis.seek(1024 * 1024 * 128);/*b kb mb*/
        IOUtils.copyBytes(fsdis, fos, configuration, true);

        /*之后用windows CMD命令将两个文件块合并
        *type block2 >> block1
        */
    }

}
