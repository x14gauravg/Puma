package com.ppe.processor;

import software.amazon.kinesis.retrieval.KinesisClientRecord;

import java.io.IOException;
import java.util.function.BiConsumer;

public interface IProcessor extends BiConsumer<AccountContext,byte[]> {

    void processUserRecord(KinesisClientRecord r) throws IOException;
    String registerPlanClass();
    BiConsumer<AccountContext,byte[]> registerHandler();
}
