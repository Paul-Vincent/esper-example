package com.madeup.esper.example;

import com.espertech.esper.client.ConfigurationEventTypeAvro;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.scopetest.SupportUpdateListener;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

public class GroupByAvroExample {

  private static final String ORDER_SCHEMA = ""
      + "{\"namespace\": \"example.avro\",\n"
      + " \"type\": \"record\",\n"
      + " \"name\": \"Order\",\n"
      + " \"fields\": [\n"
      + "     {\"name\": \"itemName\", \"type\": {\"type\": \"string\", \"avro.java.string\": \"String\"}},\n"
      + "     {\"name\": \"timestamp\", \"type\": {\"type\": \"string\", \"avro.java.string\": \"String\"}},\n"
      + "     {\"name\": \"price\",  \"type\": [\"double\", \"null\"]}"
      + " ]\n"
      + "}";

  private static final String EXPRESSION_1 ="select * from OrderEvent"
      + "#groupwin(itemName)"
      + "#time(5 seconds)"
      + "#expr_batch(current_count >= 2) "
      + "group by itemName "
      + "output first every 4 seconds";

  private static final String EXPRESSION_2 ="select * from OrderEvent"
      + "#groupwin(com.madeup.esper.example.GroupByAvroExample.combine(itemName))"
      + "#ext_timed(cast(timestamp, long, dateformat: 'iso'), 10 seconds)"
      + "#expr_batch(current_count >= 2) "
      + "group by com.madeup.esper.example.GroupByAvroExample.combine(itemName) "
      + "output first every 6 seconds";

  public static void main(String[] args) {
    EPServiceProvider epService = EPServiceProviderManager.getDefaultProvider();

    Schema orderSchema = Schema.parse(ORDER_SCHEMA);
    final ConfigurationEventTypeAvro avroEvent = new ConfigurationEventTypeAvro(orderSchema);
    epService.getEPAdministrator().getConfiguration().addEventTypeAvro("OrderEvent", avroEvent);

    EPStatement statement = epService.getEPAdministrator().createEPL(EXPRESSION_2);

    SupportUpdateListener listener = new SupportUpdateListener();
    statement.addListener(listener);

    GenericRecord event1 = new GenericData.Record(orderSchema);
    event1.put("itemName", "shirt");
    event1.put("timestamp", "2017-09-22T18:20:40.000Z");
    event1.put("price", 222.22);

    GenericRecord event2 = new GenericData.Record(orderSchema);
    event2.put("itemName", "pants");
    event2.put("timestamp", "2017-09-22T18:20:40.000Z");
    event2.put("price", 111.11);

    epService.getEPRuntime().sendEventAvro(event1, "OrderEvent");
    epService.getEPRuntime().sendEventAvro(event2, "OrderEvent");

    event1 = new GenericData.Record(orderSchema);
    event1.put("itemName", "shirt");
    event1.put("price", 222.22);
    event1.put("timestamp", "2017-09-22T18:20:41.000Z");

    event2 = new GenericData.Record(orderSchema);
    event2.put("itemName", "pants");
    event2.put("price", 111.11);
    event2.put("timestamp", "2017-09-22T18:20:41.000Z");

    epService.getEPRuntime().sendEventAvro(event1, "OrderEvent");
    epService.getEPRuntime().sendEventAvro(event2, "OrderEvent");

    event1 = new GenericData.Record(orderSchema);
    event1.put("itemName", "shirt");
    event1.put("price", 222.22);
    event1.put("timestamp", "2017-09-22T18:20:42.000Z");

    event2 = new GenericData.Record(orderSchema);
    event2.put("itemName", "pants");
    event2.put("price", 111.11);
    event2.put("timestamp", "2017-09-22T18:20:42.000Z");

    epService.getEPRuntime().sendEventAvro(event1, "OrderEvent");
    epService.getEPRuntime().sendEventAvro(event2, "OrderEvent");

    event1 = new GenericData.Record(orderSchema);
    event1.put("itemName", "shirt");
    event1.put("price", 222.22);
    event1.put("timestamp", "2017-09-22T18:20:43.000Z");

    event2 = new GenericData.Record(orderSchema);
    event2.put("itemName", "pants");
    event2.put("price", 111.11);
    event2.put("timestamp", "2017-09-22T18:20:43.000Z");

    epService.getEPRuntime().sendEventAvro(event1, "OrderEvent");
    epService.getEPRuntime().sendEventAvro(event2, "OrderEvent");

    System.out.println(listener.getNewDataList().get(0)[0].getUnderlying());
    System.out.println(listener.getNewDataList().get(0)[1].getUnderlying());
    System.out.println(listener.getNewDataList().get(1)[0].getUnderlying());
    System.out.println(listener.getNewDataList().get(1)[1].getUnderlying());
  }

  public static String combine(CharSequence name){
    return name.toString();
  }
}
