package org.wso2.cepperf.distprof;

/**
 * Created by miyurud on 3/8/17.
 */
public class FailureRecorder {
    public static void main(String[] args){
        if(args.length != 2){
            System.out.println("Usage: FailureRecorder <perf_db_host_name> <comments_on_the_benchmarking_session>");
        }else {
            DistProfAPI api = new DistProfAPI(args[0], args[1], -1);
            api.insertFailureRecord(System.currentTimeMillis());
        }
    }
}
