package storm.kafka;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;


public class IntermediateBolt extends BaseBasicBolt {
    private static final long serialVersionUID = 1L;

    public void execute(Tuple input, BasicOutputCollector collector) {
        String message=input.getString(0);

        //2016-04-26 16:40:47,174 description='LOW_PRIORITY_ALERT'  details='{"data":[{"type":"EXPECTED_ALERT"," count":59}],"scheme":1}
        String[] messageParts = message.split(" ");

        DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
        Date date = null;
        try {
            date = format.parse(messageParts[0]+ " " + messageParts[1]);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        String jsonString = messageParts[4].split("=")[1]+messageParts[5];

        HashMap<String,Object> result = null;
        try {
            result =  new ObjectMapper().readValue(jsonString.substring(1,jsonString.length()-1), HashMap.class);
        } catch (IOException e) {
            e.printStackTrace();
        }

        ArrayList al = (ArrayList) result.get("data");
        HashMap hm = (HashMap) al.get(0);

        LogMessage lm = new LogMessage();
        lm.setName(messageParts[2].split("=")[1]);
        lm.setType((String) hm.get("type"));
        lm.setUnixTimestamp(date.getTime());
        lm.setCount((Integer) hm.get("count"));

        collector.emit(new Values(lm));

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("str2"));
    }
}