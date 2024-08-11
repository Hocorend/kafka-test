package utility;

import org.slf4j.Logger;

public class LoggerFactory {

    public static Logger getLogger(){
        return org.slf4j.LoggerFactory.getLogger("main");
    }
}
