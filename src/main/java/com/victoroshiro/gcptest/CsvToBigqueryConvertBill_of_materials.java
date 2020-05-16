package com.victoroshiro.gcptest;

import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.victoroshiro.gcptest.CsvToBigqueryConvertComp_boss.FormatForBigquery;

public class CsvToBigqueryConvertBill_of_materials {
	private static final Logger LOG = LoggerFactory.getLogger(CsvToBigqueryConvert.class);
    private static String HEADERS = "tube_assembly_id,component_id_1,quantity_1,component_id_2,quantity_2,component_id_3,quantity_3,"
    		+ "component_id_4,quantity_4,component_id_5,quantity_5,component_id_6,quantity_6,component_id_7,quantity_7,component_id_8,quantity_8";

   public static class FormatForBigquery extends DoFn<String, TableRow> {

       private String[] columnNames = HEADERS.split(",");

       @ProcessElement
        public void processElement(ProcessContext c) {
            TableRow row = new TableRow();
            String[] parts = c.element().split(",");

           if (!c.element().contains(HEADERS)) {
                for (int i = 0; i < parts.length; i++) {
                    row.set(columnNames[i], parts[i]);
                }
                c.output(row);
            }
        }

       static TableSchema getSchema() {
            List<TableFieldSchema> fields = new ArrayList<>();
            // Currently store all values as String
            fields.add(new TableFieldSchema().setName("tube_assembly_id").setType("STRING"));
            fields.add(new TableFieldSchema().setName("component_id_1").setType("STRING"));
            fields.add(new TableFieldSchema().setName("quantity_1").setType("STRING"));
            fields.add(new TableFieldSchema().setName("component_id_2").setType("STRING"));
            fields.add(new TableFieldSchema().setName("quantity_2").setType("STRING"));
            fields.add(new TableFieldSchema().setName("component_id_3").setType("STRING"));
            fields.add(new TableFieldSchema().setName("quantity_3").setType("STRING"));
            fields.add(new TableFieldSchema().setName("component_id_4").setType("STRING"));
            fields.add(new TableFieldSchema().setName("quantity_4").setType("STRING"));
            fields.add(new TableFieldSchema().setName("component_id_5").setType("STRING"));
            fields.add(new TableFieldSchema().setName("quantity_5").setType("STRING"));
            fields.add(new TableFieldSchema().setName("component_id_6").setType("STRING"));
            fields.add(new TableFieldSchema().setName("quantity_6").setType("STRING"));
            fields.add(new TableFieldSchema().setName("component_id_7").setType("STRING"));
            fields.add(new TableFieldSchema().setName("quantity_7").setType("STRING"));
            fields.add(new TableFieldSchema().setName("component_id_8").setType("STRING"));
            fields.add(new TableFieldSchema().setName("quantity_8").setType("STRING"));

           return new TableSchema().setFields(fields);
        }
    }

   public static void main(String[] args) throws Throwable {
   	
        String sourceFilePath = "gs://teste-dotz/bill_of_materials.csv";
        String tempLocationPath = "gs://poc-dataflow-temp-test/bq";
        boolean isStreaming = false;
        TableReference tableRef = new TableReference();
        tableRef.setProjectId("teste-dotz-277323");
        tableRef.setDatasetId("dotz_data");
        tableRef.setTableId("bill_of_materials");

       PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
        options.setTempLocation(tempLocationPath);
        options.setJobName("csvtobq_bill_of_materials");
        Pipeline p = Pipeline.create(options);
        
       p.apply("Read CSV File", TextIO.read().from(sourceFilePath))
                .apply("Log messages", ParDo.of(new DoFn<String, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        LOG.info("Processing row: " + c.element());
                        c.output(c.element());
                    }
                })).apply("Convert to BigQuery TableRow", ParDo.of(new FormatForBigquery()))
                .apply("Write into BigQuery",
                        BigQueryIO.writeTableRows().to(tableRef).withSchema(FormatForBigquery.getSchema())
                                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                                .withWriteDisposition(isStreaming ? BigQueryIO.Write.WriteDisposition.WRITE_APPEND
                                        : BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));

       p.run().waitUntilFinish();

   }
}
