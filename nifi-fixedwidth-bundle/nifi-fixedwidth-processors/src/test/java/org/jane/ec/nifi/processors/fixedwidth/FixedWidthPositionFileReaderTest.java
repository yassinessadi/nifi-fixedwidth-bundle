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

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class FixedWidthPositionFileReaderTest {

    private TestRunner testRunner;

    @BeforeEach
    public void init() {
        testRunner = TestRunners.newTestRunner(FixedWidthPositionFileReader.class);
    }

    @Test
    public void testFixedWidthToCsvConversion() throws Exception {
        // Arrange: define the schema
        String schema = "[{\"name\":\"id\",\"start\":0,\"length\":3}," +
                        "{\"name\":\"name\",\"start\":3,\"length\":10}," +
                        "{\"name\":\"phone\",\"start\":13,\"length\":10}]";

        // Set processor properties
        testRunner.setProperty(FixedWidthPositionFileReader.SCHEMA_SERVICE_PROPERTY, schema);
        testRunner.setProperty(FixedWidthPositionFileReader.DELIMITER_PROPERTY, ",");

        // Define fixed-width input (note the spacing)
        String input = "001John Doe  1234567890\n" +  // name is exactly 10 characters
                       "002Alice     9876543210\n" +
                       "007Jane      0807771006"; 

        // Enqueue FlowFile
        testRunner.enqueue(input.getBytes(StandardCharsets.UTF_8));

        // Act
        testRunner.run();

        // Assert: ensure success
        testRunner.assertTransferCount(FixedWidthPositionFileReader.REL_SUCCESS, 1);
        testRunner.assertTransferCount(FixedWidthPositionFileReader.REL_FAILURE, 0);

        List<MockFlowFile> results = testRunner.getFlowFilesForRelationship(FixedWidthPositionFileReader.REL_SUCCESS);
        MockFlowFile result = results.get(0);

        String output = new String(result.toByteArray(), StandardCharsets.UTF_8).replace("\r\n", "\n").trim();
        String expected =
                "id,name,phone\n" +
                "001,John Doe,1234567890\n" +
                "002,Alice,9876543210\n" +
                "007,Jane,0807771006";

        assertEquals(expected, output);
    }
}
