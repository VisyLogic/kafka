package com.eventflow.activity;

import com.eventflow.shared.*;
import java.math.*;
import java.util.*;

/** Pure calculation function: easy to read and test without AWS or Kafka. */
public final class Accounting {
    public static List<Map<String,Object>> calculate(Map<String,Object> loan,Map<String,Object> event,Map<String,Object> app){
        BigDecimal principal=new BigDecimal(Json.text(loan,"principal"));
        if(principal.signum()<0)throw new IllegalArgumentException("Principal cannot be negative");
        BigDecimal rate=new BigDecimal(Json.text(app,"rate"));
        BigDecimal eventFactor=BigDecimal.ONE;
        if(event.get("config") instanceof Map<?,?> config && config.get("eventRateFactors") instanceof Map<?,?> factors)
            eventFactor=new BigDecimal(Objects.toString(factors.get(event.get("eventType")),"1"));
        BigDecimal current=new BigDecimal(Json.text(loan,"currentInterest")),prior=new BigDecimal(Json.text(loan,"priorInterest"));
        var result=new ArrayList<Map<String,Object>>();int order=0;
        for(Object be:(List<?>)app.get("businessEvents")){
            // Simple teaching rules: principal charge, current interest, interest movement.
            BigDecimal raw=switch(order){case 0->principal.multiply(rate).multiply(eventFactor);case 1->current;default->current.subtract(prior);};
            BigDecimal amount=raw.setScale(2,RoundingMode.HALF_EVEN);String value=amount.abs().toPlainString();
            String debit=Json.text(app,"debitAccount"),credit=Json.text(app,"creditAccount");if(amount.signum()<0){String swap=debit;debit=credit;credit=swap;}
            var lines=List.of(Map.of("account",debit,"debit",value,"credit","0.00"),Map.of("account",credit,"debit","0.00","credit",value));
            var calculation=Map.of("principal",principal.toPlainString(),"rate",rate.toPlainString(),"eventFactor",eventFactor.toPlainString(),"currentInterest",current.toPlainString(),"priorInterest",prior.toPlainString(),"rawAmount",raw.toPlainString(),"amount",amount.toPlainString());
            var record=new LinkedHashMap<String,Object>();record.put("businessEvent",be);record.put("sequence",++order);record.put("calculation",calculation);
            record.put("BE",Map.of("code",be,"loanId",loan.get("loanId"),"application",app.get("id"),"accountingDate",event.get("accountingDate"),"amount",amount.toPlainString()));
            record.put("AR",Map.of("businessEvent",be,"currency",loan.get("currency"),"lines",lines));result.add(record);
        }return result;
    }
}
