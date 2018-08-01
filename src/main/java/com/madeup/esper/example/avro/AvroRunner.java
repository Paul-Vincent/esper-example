package com.madeup.esper.example.avro;

import com.espertech.esper.client.ConfigurationEventTypeAvro;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.scopetest.SupportUpdateListener;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

public class AvroRunner {

  private static final String item_schema_1_0 = ""
      + "{\"namespace\": \"example.avro\",\n"
      + " \"type\": \"record\",\n"
      + " \"name\": \"Order\",\n"
      + " \"fields\": [\n"
      + "      {\n"
      + "        \"name\": \"description\",\n"
      + "        \"type\": \"string\",\n"
      + "        \"default\": \"\"\n"
      + "      }\n"
      + "]}";

  private static final String item_schema_1_1 = ""
      + "{\"namespace\": \"example.avro\",\n"
      + " \"type\": \"record\",\n"
      + " \"name\": \"Order\",\n"
      + " \"fields\": [\n"
      + "      {\n"
      + "        \"name\": \"description\",\n"
      + "        \"type\": \"string\",\n"
      + "        \"default\": \"\"\n"
      + "      }\n,"
      +"       {\n"
      + "        \"name\": \"name\",\n"
      + "        \"type\": \"string\",\n"
      + "        \"default\": \"\"\n"
      + "      }\n"
      + "]}";

  private static final String order_schema_1_0 = ""
      + "{\"namespace\": \"example.avro\",\n"
      + " \"type\": \"record\",\n"
      + " \"name\": \"Order\",\n"
      + " \"fields\": [\n"
      + "     {\"name\": \"itemName\", \"type\": \"string\"},\n"
      + "       {\n"
      + "        \"name\": \"item\",\n"
      + "        \"type\": {\n"
      + "        \"type\": \"record\",\n"
      + "        \"name\": \"ItemRef\",\n"
      + "        \"fields\": [\n"
      + "             {\n"
      + "              \"name\": \"description\",\n"
      + "              \"type\": \"string\",\n"
      + "              \"default\": \"\"\n"
      + "             }"
      + "        ]\n"
      + "       }\n"
      +       "},"
      + "     {\"name\": \"price\",  \"type\": [\"double\", \"null\"]}"
      + " ]\n"
      + "}";

  private static final String order_schema_1_1 = ""
      + "{\"namespace\": \"example.avro\",\n"
      + " \"type\": \"record\",\n"
      + " \"name\": \"Order\",\n"
      + " \"fields\": [\n"
      + "     {\"name\": \"itemName\", \"type\": \"string\"},\n"
            + "{\n"
            + "  \"name\": \"item\",\n"
            + "  \"type\": {\n"
            + "    \"type\": \"record\",\n"
            + "    \"name\": \"ItemRef\",\n"
            + "    \"fields\": [\n"
            + "      {\n"
            + "        \"name\": \"description\",\n"
            + "        \"type\": \"string\",\n"
            + "        \"default\": \"\"\n"
            + "      }\n,"
            + "      {\n"
            + "        \"name\": \"name\",\n"
            + "        \"type\": \"string\",\n"
            + "        \"default\": \"\"\n"
            + "      }]\n"
            + "  }\n"
      +       "},"
      + "     {\"name\": \"price\",  \"type\": [\"double\", \"null\"]},\n"
      + "     {\"name\": \"itemNameAlt\", \"type\": \"string\", \"default\": \"\"}"
      + " ]\n"
      + "}";

  private static final String order_schema_2_0 = ""
      + "{\"namespace\": \"example.avro\",\n"
      + " \"type\": \"record\",\n"
      + " \"name\": \"Order\",\n"
      + " \"fields\": [\n"
      + "     {\"name\": \"description\", \"type\": \"int\"},\n"
      + "     {\"name\": \"price\",  \"type\": [\"double\", \"null\"]}"
      + " ]\n"
      + "}";

  public static void main(String[] args) {
    EPServiceProvider epService = EPServiceProviderManager.getDefaultProvider();

    Schema item_1_0 = Schema.parse(item_schema_1_0);
    Schema item_1_1 = Schema.parse(item_schema_1_1);
    Schema order_1_0 = Schema.parse(order_schema_1_0);
    Schema order_1_1 = Schema.parse(order_schema_1_1);
    Schema order_2_0 = Schema.parse(order_schema_2_0);

    final ConfigurationEventTypeAvro avroEvent = new ConfigurationEventTypeAvro(order_1_0);
    epService.getEPAdministrator().getConfiguration().addEventTypeAvro("OrderEvent", avroEvent);

    String expression = "select * from OrderEvent where item.description='details' and price=256.42";
    EPStatement statement = epService.getEPAdministrator().createEPL(expression);

    final SupportUpdateListener listener = new SupportUpdateListener();
    statement.addListener(listener);

    GenericRecord itemRecord_1_0 = new GenericData.Record(item_1_0);
    itemRecord_1_0.put("description", "details");

    GenericRecord event_1_0 = new GenericData.Record(order_1_0);
    event_1_0.put("itemName", "shirt");
    event_1_0.put("item", itemRecord_1_0);
    event_1_0.put("price", 256.42);

    GenericRecord itemRecord_1_1 = new GenericData.Record(item_1_1);
    itemRecord_1_1.put("description", "details");
    itemRecord_1_1.put("name", "toy");

    GenericRecord event_1_1 = new GenericData.Record(order_1_1);
    event_1_1.put("itemNameAlt", "top");
    event_1_1.put("itemName", "shirt");
    event_1_1.put("item", itemRecord_1_1);
    event_1_1.put("price", 256.42);

    GenericRecord event_2_0 = new GenericData.Record(order_2_0);
    event_2_0.put("description", 2);
    event_2_0.put("price", 256.42);

    System.out.println("Sent Event Version 1.0: " + event_1_0.toString());
    try {
      epService.getEPRuntime().sendEventAvro(event_1_0, "OrderEvent");
    } catch (Exception e){
      System.out.println("Error Sending Event Version 1.0");
    }
    try {
      System.out.println("Returned Event Version 1.0: " + listener.getNewDataList().get(0)[0].getUnderlying().toString());
    } catch (Exception e) {
      System.out.println("Returned Event Version 1.0: No Data");
    }

    System.out.println("Sent Event Version 1.1: " + event_1_1.toString());
    try {
      epService.getEPRuntime().sendEventAvro(event_1_1, "OrderEvent");
    } catch (Exception e){
      System.out.println("Error Sending Event Version 1.1");
    }
    try {
      System.out.println("Returned Event Version 1.1: " + listener.getNewDataList().get(1)[0].getUnderlying().toString());
    } catch (Exception e) {
      System.out.println("Returned Event Version 1.1: No Data");
    }

    System.out.println("Sent Event Version 2.0: " + event_1_1.toString());
    try {
      epService.getEPRuntime().sendEventAvro(event_2_0, "OrderEvent");
    } catch (Exception e){
      System.out.println("Error Sending Event Version 2.0");
    }
    try {
      System.out.println("Returned Event Version 2.0: " + listener.getNewDataList().get(2)[0].getUnderlying().toString());
    } catch (Exception e) {
      System.out.println("Returned Event Version 2.0: No Data");
    }
  }

}
