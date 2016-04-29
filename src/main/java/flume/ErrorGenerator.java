package flume;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

public class ErrorGenerator {

    public static String generateString(){
        String finalString = "";
        final String date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS").format(new Date());
        final String[] descriptions = new String[] {"LOW_PRIORITY_ALERT", "MEDIUM_PRIORITY_ALERT", "HIGH_PRIORITY_ALERT"};
        final String[] types = new String[] {"EXPECTED_ALERT", "UNEXPECTED_ALERT"};
        final Random rand = new Random();

        finalString+=date;
        finalString+=" description='"+descriptions[rand.nextInt(descriptions.length)]+"' ";
        finalString+=" details='{\"data\":[{\"type\":\""+types[rand.nextInt(types.length)]+"\",";
        finalString+="\" count\":"+rand.nextInt(100)+"}],\"scheme\":1}'";
        return finalString;
    }
}