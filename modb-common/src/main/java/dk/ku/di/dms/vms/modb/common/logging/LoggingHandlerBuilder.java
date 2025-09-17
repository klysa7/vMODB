package dk.ku.di.dms.vms.modb.common.logging;

import dk.ku.di.dms.vms.modb.common.utils.ConfigUtils;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Date;

public final class LoggingHandlerBuilder {

    public static ILoggingHandler build(String identifier) {
        String fileName = identifier + "_" + new Date().getTime() +".llog";
        String userHome = ConfigUtils.getUserHome();
        String basePath = userHome + "/vms";
        File file = new File(basePath);
        boolean fileExists = file.exists() || file.mkdirs();
        if(!fileExists){
            throw new RuntimeException("It was not possible to create the file "+file);
        }
        String filePath = basePath + "/" + fileName;
        Path path = Paths.get(filePath);
        FileChannel fileChannel;
        try {
            fileChannel = FileChannel.open(path,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        String loggingType = System.getProperty("logging_type");
        ILoggingHandler handler;
        try {
            if(loggingType == null || loggingType.isEmpty() || loggingType.contentEquals("default")){
                handler = new DefaultLoggingHandler(fileChannel, fileName);
            } else {
                handler = new CompressedLoggingHandler(fileChannel, fileName);
            }
        } catch (NoClassDefFoundError | Exception e) {
            System.out.println("Failed to load compressed logging handler, setting the default. Error:\n"+e);
            handler = new DefaultLoggingHandler(fileChannel, fileName);
        }
        return handler;
    }

}
