package HdfsClientApi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.net.URI;

public class TestRmdirs {
    @Test
    public void testRmdirs() throws Exception {
        Configuration configuration = new Configuration();
//        configuration.set("fs.defaultFS", "hdfs://hadoop101:9000");
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop101:9000"),
                configuration,
                "atguigu");
        fs.delete(new Path("/hadoop"), true);
        fs.close();
    }
}
