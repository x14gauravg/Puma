package com.ppe.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

import java.io.IOException;
import java.util.function.BiConsumer;

@Component
public abstract class BaseProcessor implements IProcessor {

    private String planClass;
    private AccountContext accntConfig;
    private BiConsumer<AccountContext,byte[]> processorHandler;

    public BaseProcessor() {
        setupProcessor();
    }

    public void setupProcessor(){
        this.planClass = registerPlanClass();
        this.processorHandler = registerHandler();
    }

    public void processUserRecord(KinesisClientRecord r) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        byte[] b = new byte[r.data().remaining()];
        PPEvent PPEvent = objectMapper.readValue(b, PPEvent.class);
        if(planClass.equalsIgnoreCase(PPEvent.getPlanClass())){
            // get account configuration
            // Call registered function
            processorHandler.accept(accntConfig,b);
        }

    }

}
