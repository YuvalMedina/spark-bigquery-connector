/*
 * Copyright 2018 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.spark.bigquery;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.storage.v1alpha2.ProtoBufProto;
import com.google.cloud.bigquery.storage.v1alpha2.ProtoSchemaConverter;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.AssumptionViolatedException;
import org.junit.Test;

import static com.google.cloud.spark.bigquery.ProtobufUtils.*;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.fail;

public class ProtobufUtilsTest {

    private final Logger logger = LogManager.getLogger("com.google.cloud.spark");

    @Test
    public void testBigQueryRecordToDescriptor() throws Exception {
        logger.setLevel(Level.DEBUG);

        DescriptorProtos.DescriptorProto expected = NESTED_STRUCT_DESCRIPTOR.setName("Struct").build();
        DescriptorProtos.DescriptorProto converted = buildDescriptorProtoWithFields(DescriptorProtos.DescriptorProto.newBuilder()
                .setName("Struct"), BIGQUERY_NESTED_STRUCT_FIELD.getSubFields(), 0);

        assertThat(converted).isEqualTo(expected);
    }

    @Test
    public void testBigQueryToProtoSchema() throws Exception {
        logger.setLevel(Level.DEBUG);

        ProtoBufProto.ProtoSchema converted = toProtoSchema(BIG_BIGQUERY_SCHEMA);
        ProtoBufProto.ProtoSchema expected = ProtoSchemaConverter.convert(
                Descriptors.FileDescriptor.buildFrom(
                        DescriptorProtos.FileDescriptorProto.newBuilder()
                                .addMessageType(
                                        DescriptorProtos.DescriptorProto.newBuilder()
                                                .addField(PROTO_INTEGER_FIELD.clone().setNumber(1))
                                                .addField(PROTO_STRING_FIELD.clone().setNumber(2))
                                                .addField(PROTO_ARRAY_FIELD.clone().setNumber(3))
                                                .addNestedType(NESTED_STRUCT_DESCRIPTOR.clone())
                                                .addField(PROTO_STRUCT_FIELD.clone().setNumber(4))
                                                .addField(PROTO_BYTES_FIELD.clone().setName("Geography").setNumber(5))
                                                .addField(PROTO_DOUBLE_FIELD.clone().setName("Float").setNumber(6))
                                                .addField(PROTO_BOOLEAN_FIELD.clone().setNumber(7))
                                                .setName("Schema").build()
                                ).build(), new Descriptors.FileDescriptor[]{}
                ).getMessageTypes().get(0)
        );

        logger.debug("Expected schema: "+expected.getProtoDescriptor());
        logger.debug("Actual schema: "+converted.getProtoDescriptor());

        for(int i = 0; i < 7; i++){
            assertThat(converted.getProtoDescriptor().getField(i)).isEqualTo(expected.getProtoDescriptor().getField(i));
        }
    }

    @Test
    public void testSparkIntegerSchemaToDescriptor() throws Exception {
        logger.setLevel(Level.DEBUG);

        StructType schema = new StructType().add(SPARK_INTEGER_FIELD);
        DescriptorProtos.DescriptorProto converted = toDescriptor(schema).toProto();

        DescriptorProtos.DescriptorProto expected = DESCRIPTOR_PROTO_INTEGER;

        assertThat(converted).isEqualTo(expected);
    }

    @Test
    public void testSparkStringSchemaToDescriptor() throws Exception {
        logger.setLevel(Level.DEBUG);

        StructType schema = new StructType().add(SPARK_STRING_FIELD);
        DescriptorProtos.DescriptorProto converted = toDescriptor(schema).toProto();

        DescriptorProtos.DescriptorProto expected = DESCRIPTOR_PROTO_STRING;

        assertThat(converted).isEqualTo(expected);
    }

    @Test
    public void testSparkArraySchemaToDescriptor() throws Exception {
        logger.setLevel(Level.DEBUG);

        StructType schema = new StructType().add(SPARK_ARRAY_FIELD);
        DescriptorProtos.DescriptorProto converted = toDescriptor(schema).toProto();

        DescriptorProtos.DescriptorProto expected = DESCRIPTOR_PROTO_ARRAY;

        assertThat(converted).isEqualTo(expected);
    }

    @Test
    public void testSparkNestedStructSchemaToDescriptor() throws Exception {
        logger.setLevel(Level.DEBUG);

        StructType schema = new StructType().add(SPARK_NESTED_STRUCT_FIELD);
        DescriptorProtos.DescriptorProto converted = toDescriptor(schema).toProto();

        DescriptorProtos.DescriptorProto expected = DESCRIPTOR_PROTO_STRUCT;

        assertThat(converted).isEqualTo(expected);
    }

    @Test
    public void testSparkArrayRowToDynamicMessage() throws Exception {
        logger.setLevel(Level.DEBUG);

        StructType schema = new StructType().add(SPARK_ARRAY_FIELD);
        DynamicMessage converted = createSingleRowMessage(schema, toDescriptor(schema),
                ARRAY_INTERNAL_ROW);
        DynamicMessage expected = ARRAY_ROW_MESSAGE;

        assertThat(converted.toString()).isEqualTo(expected.toString());
    }

    @Test
    public void testSparkStructRowToDynamicMessage() throws Exception {
        logger.setLevel(Level.DEBUG);

        StructType schema = new StructType().add(SPARK_NESTED_STRUCT_FIELD);
        DynamicMessage converted = createSingleRowMessage(schema, toDescriptor(schema),
                STRUCT_INTERNAL_ROW);
        DynamicMessage expected = StructRowMessage;

        assertThat(converted.toString()).isEqualTo(expected.toString());
    }

    @Test
    public void testSparkRowToProtoRow() throws Exception {
        logger.setLevel(Level.DEBUG);

        ProtoBufProto.ProtoRows converted = toProtoRows(BIG_SPARK_SCHEMA,
                new InternalRow[]{
                        new GenericInternalRow(new Object[]{
                                1,
                                "A",
                                ArrayData.toArrayData(new int[]{0,1,2}),
                                INTERNAL_STRUCT_DATA,
                                3.14,
                                true
                        })}
        );

        ProtoBufProto.ProtoRows expected = MyProtoRows;

        assertThat(converted.getSerializedRows(0).toByteArray()).isEqualTo(expected.getSerializedRows(0).toByteArray());
    }

    @Test
    public void testSettingARequiredFieldAsNull() throws Exception {
        logger.setLevel(Level.DEBUG);

        try {
            convert(SPARK_STRING_FIELD, null);
            fail("Convert did not assert field's /'Required/' status");
        } catch (IllegalArgumentException e){}
        try {
            convert(new StructField("String", DataTypes.StringType, true, Metadata.empty()),
                    null);
        } catch (Exception e) {
            fail("A nullable field could not be set to null.");
        }
    }



    private final StructType MY_STRUCT = DataTypes.createStructType(
            new StructField[]{new StructField("Number", DataTypes.IntegerType,
                    true, Metadata.empty()),
                    new StructField("String", DataTypes.StringType,
                            true, Metadata.empty())});

    private final StructField SPARK_INTEGER_FIELD = new StructField("Number", DataTypes.IntegerType,
            true, Metadata.empty());
    private final StructField SPARK_STRING_FIELD = new StructField("String", DataTypes.StringType,
            false, Metadata.empty());
    private final StructField SPARK_NESTED_STRUCT_FIELD = new StructField("Struct", MY_STRUCT,
            true, Metadata.empty());
    private final StructField SPARK_ARRAY_FIELD = new StructField("Array",
            DataTypes.createArrayType(DataTypes.IntegerType),
            true, Metadata.empty());
    private final StructField SPARK_DOUBLE_FIELD = new StructField("Double", DataTypes.DoubleType,
            true, Metadata.empty());
    private final StructField SPARK_BOOLEAN_FIELD = new StructField("Boolean", DataTypes.BooleanType,
            true, Metadata.empty());

    private final StructType BIG_SPARK_SCHEMA = new StructType()
            .add(SPARK_INTEGER_FIELD)
            .add(SPARK_STRING_FIELD)
            .add(SPARK_ARRAY_FIELD)
            .add(SPARK_NESTED_STRUCT_FIELD)
            .add(SPARK_DOUBLE_FIELD)
            .add(SPARK_BOOLEAN_FIELD);


    private final Field BIGQUERY_INTEGER_FIELD = Field.newBuilder("Number", LegacySQLTypeName.INTEGER,
            (FieldList)null).setMode(Field.Mode.NULLABLE).build();
    private final Field BIGQUERY_STRING_FIELD = Field.newBuilder("String", LegacySQLTypeName.STRING, (FieldList) null)
            .setMode(Field.Mode.REQUIRED).build();
    private final Field BIGQUERY_NESTED_STRUCT_FIELD = Field.newBuilder("Struct", LegacySQLTypeName.RECORD,
            Field.newBuilder("Number", LegacySQLTypeName.INTEGER, (FieldList) null)
                    .setMode(Field.Mode.NULLABLE).build(),
            Field.newBuilder("String", LegacySQLTypeName.STRING, (FieldList) null)
                    .setMode(Field.Mode.NULLABLE).build())
            .setMode(Field.Mode.NULLABLE).build();
    private final Field BIGQUERY_ARRAY_FIELD = Field.newBuilder("Array", LegacySQLTypeName.INTEGER, (FieldList) null)
            .setMode(Field.Mode.REPEATED).build();
    private final Field BIGQUERY_GEOGRAPHY_FIELD = Field.newBuilder("Geography", LegacySQLTypeName.GEOGRAPHY, (FieldList)null)
            .setMode(Field.Mode.NULLABLE).build();
    private final Field BIGQUERY_FLOAT_FIELD = Field.newBuilder("Float", LegacySQLTypeName.FLOAT, (FieldList)null)
            .setMode(Field.Mode.NULLABLE).build();
    private final Field BIGQUERY_BOOLEAN_FIELD = Field.newBuilder("Boolean", LegacySQLTypeName.BOOLEAN, (FieldList)null)
            .setMode(Field.Mode.NULLABLE).build();

    private final Schema BIG_BIGQUERY_SCHEMA = Schema.of(BIGQUERY_INTEGER_FIELD, BIGQUERY_STRING_FIELD, BIGQUERY_ARRAY_FIELD, BIGQUERY_NESTED_STRUCT_FIELD,
            BIGQUERY_GEOGRAPHY_FIELD, BIGQUERY_FLOAT_FIELD, BIGQUERY_BOOLEAN_FIELD);


    private final DescriptorProtos.FieldDescriptorProto.Builder PROTO_INTEGER_FIELD = DescriptorProtos.FieldDescriptorProto.newBuilder()
            .setName("Number")
            .setNumber(1)
            .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64)
            .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL);
    private final DescriptorProtos.FieldDescriptorProto.Builder PROTO_STRING_FIELD = DescriptorProtos.FieldDescriptorProto.newBuilder()
            .setName("String")
            .setNumber(1)
            .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
            .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REQUIRED);
    private final DescriptorProtos.FieldDescriptorProto.Builder PROTO_ARRAY_FIELD = DescriptorProtos.FieldDescriptorProto.newBuilder()
            .setName("Array")
            .setNumber(1)
            .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64)
            .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED);
    private final DescriptorProtos.DescriptorProto.Builder NESTED_STRUCT_DESCRIPTOR = DescriptorProtos.DescriptorProto.newBuilder()
            .setName("STRUCT1")
            .addField(PROTO_INTEGER_FIELD.clone())
            .addField(PROTO_STRING_FIELD.clone().setNumber(2)
                    .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));
    private final DescriptorProtos.FieldDescriptorProto.Builder PROTO_STRUCT_FIELD = DescriptorProtos.FieldDescriptorProto.newBuilder()
            .setName("Struct")
            .setNumber(1)
            .setTypeName("STRUCT1")
            .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL);
    private final DescriptorProtos.FieldDescriptorProto.Builder PROTO_BYTES_FIELD = DescriptorProtos.FieldDescriptorProto.newBuilder()
            .setName("Bytes")
            .setNumber(1)
            .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_BYTES)
            .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL);
    private final DescriptorProtos.FieldDescriptorProto.Builder PROTO_DOUBLE_FIELD = DescriptorProtos.FieldDescriptorProto.newBuilder()
            .setName("Double")
            .setNumber(1)
            .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_DOUBLE)
            .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL);
    private final DescriptorProtos.FieldDescriptorProto.Builder PROTO_BOOLEAN_FIELD = DescriptorProtos.FieldDescriptorProto.newBuilder()
            .setName("Boolean")
            .setNumber(1)
            .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_BOOL)
            .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL);


    private final DescriptorProtos.DescriptorProto DESCRIPTOR_PROTO_INTEGER = DescriptorProtos.DescriptorProto.newBuilder()
            .addField(PROTO_INTEGER_FIELD).setName("Schema").build();
    private final DescriptorProtos.DescriptorProto DESCRIPTOR_PROTO_STRING = DescriptorProtos.DescriptorProto.newBuilder()
            .addField(PROTO_STRING_FIELD).setName("Schema").build();
    private final DescriptorProtos.DescriptorProto DESCRIPTOR_PROTO_ARRAY = DescriptorProtos.DescriptorProto.newBuilder()
            .addField(PROTO_ARRAY_FIELD).setName("Schema").build();
    private final DescriptorProtos.DescriptorProto DESCRIPTOR_PROTO_STRUCT = DescriptorProtos.DescriptorProto.newBuilder()
            .addNestedType(NESTED_STRUCT_DESCRIPTOR).addField(PROTO_STRUCT_FIELD).setName("Schema").build();

    private final InternalRow INTEGER_INTERNAL_ROW = new GenericInternalRow(new Object[]{1});
    private final InternalRow STRING_INTERNAL_ROW = new GenericInternalRow(new Object[]{"A"});
    private final InternalRow ARRAY_INTERNAL_ROW = new GenericInternalRow(new Object[]{ArrayData.toArrayData(
            new int[]{0,1,2})});
    private final InternalRow INTERNAL_STRUCT_DATA = new GenericInternalRow(new Object[]{1, "A"});
    private final InternalRow STRUCT_INTERNAL_ROW = new GenericInternalRow(new Object[]{INTERNAL_STRUCT_DATA});


    private Descriptors.Descriptor INTEGER_SCHEMA_DESCRIPTOR = createIntegerSchemaDescriptor();
    private Descriptors.Descriptor createIntegerSchemaDescriptor() {
        try {
            return toDescriptor(
                    new StructType().add(SPARK_INTEGER_FIELD)
            );
        } catch (Descriptors.DescriptorValidationException e) {
            throw new AssumptionViolatedException("Could not create INTEGER_SCHEMA_DESCRIPTOR", e);
        }
    }
    private Descriptors.Descriptor STRING_SCHEMA_DESCRIPTOR = createStringSchemaDescriptor();
    private Descriptors.Descriptor createStringSchemaDescriptor() {
        try {
            return toDescriptor(
                    new StructType().add(SPARK_STRING_FIELD)
            );
        } catch (Descriptors.DescriptorValidationException e) {
            throw new AssumptionViolatedException("Could not create STRING_SCHEMA_DESCRIPTOR", e);
        }
    }
    private Descriptors.Descriptor ARRAY_SCHEMA_DESCRIPTOR = createArraySchemaDescriptor();
    private Descriptors.Descriptor createArraySchemaDescriptor() {
        try {
            return toDescriptor(
                    new StructType().add(SPARK_ARRAY_FIELD)
            );
        } catch (Descriptors.DescriptorValidationException e) {
            throw new AssumptionViolatedException("Could not create ARRAY_SCHEMA_DESCRIPTOR", e);
        }
    }
    private Descriptors.Descriptor STRUCT_SCHEMA_DESCRIPTOR = createStructSchemaDescriptor();
    private Descriptors.Descriptor createStructSchemaDescriptor() {
        try {
            return toDescriptor(
                    new StructType().add(SPARK_NESTED_STRUCT_FIELD)
            );
        } catch (Descriptors.DescriptorValidationException e) {
            throw new AssumptionViolatedException("Could not create STRUCT_SCHEMA_DESCRIPTOR", e);
        }
    }


    private final DynamicMessage INTEGER_ROW_MESSAGE = DynamicMessage.newBuilder(INTEGER_SCHEMA_DESCRIPTOR)
            .setField(INTEGER_SCHEMA_DESCRIPTOR.findFieldByNumber(1), 1L).build();
    private final DynamicMessage STRING_ROW_MESSAGE = DynamicMessage.newBuilder(STRING_SCHEMA_DESCRIPTOR)
            .setField(STRING_SCHEMA_DESCRIPTOR.findFieldByNumber(1), "A").build();
    private final DynamicMessage ARRAY_ROW_MESSAGE = DynamicMessage.newBuilder(ARRAY_SCHEMA_DESCRIPTOR)
            .addRepeatedField(ARRAY_SCHEMA_DESCRIPTOR.findFieldByNumber(1), 0L)
            .addRepeatedField(ARRAY_SCHEMA_DESCRIPTOR.findFieldByNumber(1), 1L)
            .addRepeatedField(ARRAY_SCHEMA_DESCRIPTOR.findFieldByNumber(1), 2L).build();
    private DynamicMessage StructRowMessage = createStructRowMessage();
    private DynamicMessage createStructRowMessage() {
        try{
            return DynamicMessage.newBuilder(STRUCT_SCHEMA_DESCRIPTOR)
                    .setField(STRUCT_SCHEMA_DESCRIPTOR.findFieldByNumber(1), createSingleRowMessage(
                            MY_STRUCT, toDescriptor(MY_STRUCT), INTERNAL_STRUCT_DATA
                    )).build();
        } catch (Descriptors.DescriptorValidationException e) {
            throw new AssumptionViolatedException("Could not create STRUCT_ROW_MESSAGE", e);
        }
    }


    private Descriptors.Descriptor BIG_SCHEMA_ROW_DESCRIPTOR = createBigSchemaRowDescriptor();
    private Descriptors.Descriptor createBigSchemaRowDescriptor() {
        try {
            return toDescriptor(BIG_SPARK_SCHEMA);
        } catch (Descriptors.DescriptorValidationException e) {
            throw new AssumptionViolatedException("Could not create BIG_SCHEMA_ROW_DESCRIPTOR", e);
        }
    }
    private ProtoBufProto.ProtoRows MyProtoRows = createMyProtoRows();
    private ProtoBufProto.ProtoRows createMyProtoRows() {
        try {
            return ProtoBufProto.ProtoRows.newBuilder().addSerializedRows(
                    DynamicMessage.newBuilder(BIG_SCHEMA_ROW_DESCRIPTOR)
                            .setField(BIG_SCHEMA_ROW_DESCRIPTOR.findFieldByNumber(1), 1L)
                            .setField(BIG_SCHEMA_ROW_DESCRIPTOR.findFieldByNumber(2), "A")
                            .addRepeatedField(BIG_SCHEMA_ROW_DESCRIPTOR.findFieldByNumber(3), 0L)
                            .addRepeatedField(BIG_SCHEMA_ROW_DESCRIPTOR.findFieldByNumber(3), 1L)
                            .addRepeatedField(BIG_SCHEMA_ROW_DESCRIPTOR.findFieldByNumber(3), 2L)
                            .setField(BIG_SCHEMA_ROW_DESCRIPTOR.findFieldByNumber(4),
                                    createSingleRowMessage(
                                            MY_STRUCT, toDescriptor(MY_STRUCT), INTERNAL_STRUCT_DATA))
                            .setField(BIG_SCHEMA_ROW_DESCRIPTOR.findFieldByNumber(5), 3.14)
                            .setField(BIG_SCHEMA_ROW_DESCRIPTOR.findFieldByNumber(6), true)
                            .build().toByteString()).build();
        } catch (Descriptors.DescriptorValidationException e) {
            throw new AssumptionViolatedException("Could not create MY_PROTO_ROWS", e);
        }
    }
}
