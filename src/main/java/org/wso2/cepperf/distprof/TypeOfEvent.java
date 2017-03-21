package org.wso2.cepperf.distprof;

/**
 * Created by miyurud on 2/23/17.
 */
public enum TypeOfEvent {
    Normal(0),
    Failure(1);

    private int type;

    TypeOfEvent(int t){
        this.type = t;
    }

    public int getType(){
        return type;
    }
}
