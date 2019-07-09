package io.confluent.telemetry.exporter;

import io.confluent.telemetry.serde.ProtoToFlatJson;
import io.opencensus.proto.metrics.v1.Metric;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collection;

public class FileExporter implements Exporter {
    private String path;

    private Deserializer deserializer;

    private int count = 0;

    public FileExporter(String path) {
        this.path = path;
        this.deserializer = new ProtoToFlatJson();
    }

    @Override
    public void write(Collection<Metric> metrics) throws RuntimeException {
        FileWriter fileWriter = null;
        try {
            fileWriter = new FileWriter(this.path + "/" + count++);
        } catch (IOException e) {
            e.printStackTrace();
        }

        PrintWriter printWriter = new PrintWriter(fileWriter);
        for (Metric m : metrics) {
            try {
                printWriter.println(this.deserializer.deserialize("", m.toByteArray()));
            } catch (Exception e) {
                System.out.println("error in record : " + m);
                e.printStackTrace();
            }
        }
        printWriter.close();
    }

    @Override
    public void close() throws Exception {

    }
}
