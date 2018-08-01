package com.madeup.esper.example;

import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;

public class Runner {

    public static void main(String[] args) throws InterruptedException {
        EPServiceProvider epService = EPServiceProviderManager.getDefaultProvider();

        String expression = "select * from com.madeup.esper.example.OrderEvent(itemName='shirt') as ge limit 1";
        EPStatement statement = epService.getEPAdministrator().createEPL(expression);

        MyListener listener = new MyListener();
        statement.addListener(listener);

//        String expression2 = "select * from com.jci.taas.esper.example.OrderEvent.win:time_batch(5 sec)";
//        EPStatement statement2 = epService.getEPAdministrator().createEPL(expression2);
//
//        MyListener2 listener2 = new MyListener2();
//        statement2.addListener(listener2);

        OrderEvent event = new OrderEvent("pants", 7754.50);
        epService.getEPRuntime().sendEvent(event);
        event = new OrderEvent("shirt", 7423.50);
        epService.getEPRuntime().sendEvent(event);
        event = new OrderEvent("pants", 7433.50);
        epService.getEPRuntime().sendEvent(event);

        Thread.sleep(30000);

        event = new OrderEvent("shirt", 3.50);
        epService.getEPRuntime().sendEvent(event);
        event = new OrderEvent("shirt", 4.50);
        epService.getEPRuntime().sendEvent(event);
        event = new OrderEvent("pants", 1.50);
        epService.getEPRuntime().sendEvent(event);
        event = new OrderEvent("shirt", 7.50);
        epService.getEPRuntime().sendEvent(event);
    }

}
