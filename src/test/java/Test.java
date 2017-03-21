import org.wso2.cepperf.distprof.DistProfAPI;

/**
 * Created by miyurud on 2/23/17.
 */
public class Test {
    public static void main(String[] args){
        DistProfAPI apiObj = new DistProfAPI("zoo1","Flink benchmarking session.");
        System.out.println(apiObj.insertNormalRecord(System.currentTimeMillis(), "zoo1", 1.1f, 2.1f, 3.1f, 4.1f, 5.1f, 0));

        System.out.println(apiObj.insertFailureRecord(System.currentTimeMillis()));
        System.out.println(apiObj.getSequenceNumber());
        apiObj.shutdown();
    }
}
