package com.madeup.esper.example;

import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.scopetest.SupportUpdateListener;

public class GroupByExample {

  private static final String EXPRESSION_1 ="select * from com.jci.taas.esper.example.OrderEvent"
      + "#groupwin(itemName)"
      + "#time(1 seconds)"
      + "#expr_batch(current_count >= 1) "
      + "group by itemName "
      + "output first every 4 seconds";

  private static final String EXPRESSION_2 ="select * from com.jci.taas.esper.example.OrderEvent"
      + "#groupwin(com.madeup.esper.example.GroupByExample.combine(itemName, price))"
      + "#time(1 seconds)"
      + "#expr_batch(current_count >= 1) "
      + "group by itemName "
      + "output first every 4 seconds";

  public static void main(String[] args) {
    EPServiceProvider epService = EPServiceProviderManager.getDefaultProvider();

    EPStatement statement = epService.getEPAdministrator().createEPL(EXPRESSION_2);

    SupportUpdateListener listener = new SupportUpdateListener();
    statement.addListener(listener);


    OrderEvent event1 = new OrderEvent("shirt", 222.22);
    epService.getEPRuntime().sendEvent(event1);
    OrderEvent event2 = new OrderEvent("shirt", 222.22);
    epService.getEPRuntime().sendEvent(event2);
    System.out.println(listener.getNewDataList().get(0)[0].getUnderlying());
  }

  public static String combine(String name, Double price){
    return name + price;
  }

}
