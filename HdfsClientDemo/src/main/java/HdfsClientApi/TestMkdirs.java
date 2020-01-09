package HdfsClientApi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.net.URI;

public class TestMkdirs {
    @Test
    public void testMKdirs() throws Exception {
        Configuration configuration = new Configuration();
//        configuration.set("fs.defaultFS", "hdfs://hadoop101:9000");
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop101:9000"),
                configuration,
                "atguigu");
        fs.mkdirs(new Path("/hadoop"));
        fs.close();
    }
}
