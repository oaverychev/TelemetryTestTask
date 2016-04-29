package flume;


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

public class Main {

    public static void main(String[] args) throws  InterruptedException, IOException {
        File file = new File("/home/oaverychev/flume.log");

        if (!file.exists()) {
            file.createNewFile();
        }

        while ( true )
        {
            String finalString = "";
            final String date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS").format(new Date());
            final String[] descriptions = new String[] {"LOW_PRIORITY_ALERT", "MEDIUM_PRIORITY_ALERT", "HIGH_PRIORITY_ALERT"};
            final String[] types = new String[] {"EXPECTED_ALERT", "UNEXPECTED_ALERT"};
            final Random rand = new Random();

            finalString+=date;
            finalString+=" description='"+descriptions[rand.nextInt(descriptions.length)]+"' ";
            finalString+="details='{\"data\":[{\"type\":\""+types[rand.nextInt(types.length)]+"\",";
            finalString+="\"count\":"+rand.nextInt(100)+"}],\"scheme\":1}";

            FileWriter fw = new FileWriter(file.getAbsoluteFile(), true);
            BufferedWriter bw = new BufferedWriter(fw);
            bw.write(finalString+"\n");
            bw.close();

            System.out.println(finalString);

            Thread.sleep(1000);

        }

    }



}
