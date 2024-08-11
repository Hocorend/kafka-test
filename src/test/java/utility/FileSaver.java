package utility;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class FileSaver {

    public static void saveToFile(String content, File file) throws IOException {

        if (!file.exists()) {
            file.createNewFile();
        }

        Files.write(Paths.get(file.getPath()), content.getBytes(), StandardOpenOption.APPEND);
    }
}
