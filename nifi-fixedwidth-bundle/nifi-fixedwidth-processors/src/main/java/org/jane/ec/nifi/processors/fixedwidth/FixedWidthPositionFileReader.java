/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jane.ec.nifi.processors.fixedwidth;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;


@Tags({"text","FixedPosition","update"})
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class FixedWidthPositionFileReader extends AbstractProcessor {

    // PROPERTIES HERE
    public static final PropertyDescriptor SCHEMA_SERVICE_PROPERTY = new PropertyDescriptor
            .Builder().name("schema-fixed-postion")
            .displayName("fixed schema")
            .description("schema to parse the fixed position file")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor DELIMITER_PROPERTY = new PropertyDescriptor
            .Builder().name("sDelimiter")
            .displayName("CSV Delimiter")
            .description("Character used to separate fields in output")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
        

    // RELATIONSHIPS HERE
    public static final Relationship REL_SUCCESS =  new Relationship.Builder()
            .name("success")
            .description("All FlowFiles are routed to this relationship")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles that could not be updated are routed to this relationship")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    private static class FieldDef {
        public String name;
        public int start;
        public int length;
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        descriptors = List.of(SCHEMA_SERVICE_PROPERTY, DELIMITER_PROPERTY);
        relationships = Set.of(REL_SUCCESS, REL_FAILURE);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {}


    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) return;
        final String schemaJson = context.getProperty(SCHEMA_SERVICE_PROPERTY).getValue();
        final String delimiter = context.getProperty(DELIMITER_PROPERTY).getValue();
        ObjectMapper mapper = new ObjectMapper();

        try {
            List<FieldDef> schema = mapper.readValue(schemaJson, new TypeReference<List<FieldDef>>() {});
            List<String> headers = new ArrayList<>();
            for (FieldDef field : schema) {
                headers.add(field.name);
            }

            flowFile = session.write(flowFile, (in, out) -> {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(in));
                     BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out))) {

                    writer.write(String.join(delimiter, headers));
                    writer.newLine();

                    String line;
                    while ((line = reader.readLine()) != null) {
                        List<String> row = new ArrayList<>();
                        for (FieldDef field : schema) {
                            int start = field.start;
                            int end = Math.min(start + field.length, line.length());
                            String value = (start < line.length()) ? line.substring(start, end).trim() : "";
                            // CSV escaping
                            value = value.replace("\"", "\"\"");
                            if (value.contains(delimiter) || value.contains("\"")) {
                                value = "\"" + value + "\"";
                            }
                            row.add(value);
                        }
                        writer.write(String.join(delimiter, row));
                        writer.newLine();
                    }
                }
            });

            session.transfer(flowFile, REL_SUCCESS);
        } catch (Exception e) {
            getLogger().error("Failed to process fixed-width file", e);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

}
