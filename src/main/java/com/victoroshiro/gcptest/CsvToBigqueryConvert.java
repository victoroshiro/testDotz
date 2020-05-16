/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.victoroshiro.gcptest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TableFieldSchema;

public class CsvToBigqueryConvert {
     private static final Logger LOG = LoggerFactory.getLogger(CsvToBigqueryConvert.class);
     private static String HEADERS = "tube_assembly_id,supplier,quote_date,annual_usage,min_order_quantity,bracket_pricing,quantity,cost";

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
             fields.add(new TableFieldSchema().setName("tube_assembly_id").setType("STRING"));
             fields.add(new TableFieldSchema().setName("supplier").setType("STRING"));
             fields.add(new TableFieldSchema().setName("quote_date").setType("STRING"));
             fields.add(new TableFieldSchema().setName("annual_usage").setType("INTEGER"));
             fields.add(new TableFieldSchema().setName("min_order_quantity").setType("INTEGER"));
             fields.add(new TableFieldSchema().setName("bracket_pricing").setType("STRING"));
             fields.add(new TableFieldSchema().setName("quantity").setType("INTEGER"));
             fields.add(new TableFieldSchema().setName("cost").setType("FLOAT"));

            return new TableSchema().setFields(fields);
         }
     }

    public static void main(String[] args) throws Throwable {
    	
         String sourceFilePath = "gs://teste-dotz/price_quote.csv";
         String tempLocationPath = "gs://poc-dataflow-temp-test/bq";
         boolean isStreaming = false;
         TableReference tableRef = new TableReference();
         tableRef.setProjectId("teste-dotz-277323");
         tableRef.setDatasetId("dotz_data");
         tableRef.setTableId("price_quote");

        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
         options.setTempLocation(tempLocationPath);
         options.setJobName("csvtobq_price_quote");
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