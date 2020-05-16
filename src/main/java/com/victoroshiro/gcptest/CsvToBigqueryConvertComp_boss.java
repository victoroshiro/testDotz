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
import com.victoroshiro.gcptest.CsvToBigqueryConvert.FormatForBigquery;

public class CsvToBigqueryConvertComp_boss {
	private static final Logger LOG = LoggerFactory.getLogger(CsvToBigqueryConvert.class);
    private static String HEADERS = "component_id,component_type_id,type,connection_type_id,outside_shape,base_type,height_over_tube,bolt_pattern_long,bolt_pattern_wide,groove,base_diameter,shoulder_diameter,unique_feature,orientation,weight";

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
            fields.add(new TableFieldSchema().setName("component_id").setType("STRING"));
            fields.add(new TableFieldSchema().setName("component_type_id").setType("STRING"));
            fields.add(new TableFieldSchema().setName("type").setType("STRING"));
            fields.add(new TableFieldSchema().setName("connection_type_id").setType("STRING"));
            fields.add(new TableFieldSchema().setName("outside_shape").setType("STRING"));
            fields.add(new TableFieldSchema().setName("base_type").setType("STRING"));
            fields.add(new TableFieldSchema().setName("height_over_tube").setType("STRING"));
            fields.add(new TableFieldSchema().setName("bolt_pattern_long").setType("STRING"));
            fields.add(new TableFieldSchema().setName("bolt_pattern_wide").setType("STRING"));
            fields.add(new TableFieldSchema().setName("groove").setType("STRING"));
            fields.add(new TableFieldSchema().setName("base_diameter").setType("STRING"));
            fields.add(new TableFieldSchema().setName("shoulder_diameter").setType("STRING"));
            fields.add(new TableFieldSchema().setName("unique_feature").setType("STRING"));
            fields.add(new TableFieldSchema().setName("orientation").setType("STRING"));
            fields.add(new TableFieldSchema().setName("weight").setType("STRING"));

           return new TableSchema().setFields(fields);
        }
    }

   public static void main(String[] args) throws Throwable {
   	
        String sourceFilePath = "gs://teste-dotz/comp_boss.csv";
        String tempLocationPath = "gs://poc-dataflow-temp-test/bq";
        boolean isStreaming = false;
        TableReference tableRef = new TableReference();
        tableRef.setProjectId("teste-dotz-277323");
        tableRef.setDatasetId("dotz_data");
        tableRef.setTableId("comp_boss");

       PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
        options.setTempLocation(tempLocationPath);
        options.setJobName("csvtobq_comp_boss");
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
