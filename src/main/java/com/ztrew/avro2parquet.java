package com.ztrew;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import parquet.avro.AvroSchemaConverter;
import parquet.avro.AvroWriteSupport;
import parquet.hadoop.ParquetWriter;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.schema.MessageType;

import java.io.File;
import java.io.IOException;

/**
 * Created by cwertz001c on 10/31/16.
 */
public class avro2parquet
{
    private static Logger logger = LoggerFactory.getLogger(avro2parquet.class);

    public static void main(String[] args) throws IOException {

        String usage = "{inputFile} {outputFile}";
        if (args.length != usage.split(" ").length)
        {
            System.out.println(usage);
            return;
        }

        String inputFile = args[0];
        String outputFile = args[1];

        logger.info("Input file: " + inputFile);
        logger.info("Output file: " + outputFile);

        Configuration conf = new Configuration();

        // load your Avro schema
        Schema avroSchema = new Schema.Parser().parse(inputFile);
        File avroFile = new File(inputFile);

        // generate the corresponding Parquet schema
        MessageType parquetSchema = new AvroSchemaConverter().convert(avroSchema);

        // create a WriteSupport object to serialize your Avro objects
        AvroWriteSupport writeSupport = new AvroWriteSupport(parquetSchema, avroSchema, GenericData.get());

        // choose compression scheme
        CompressionCodecName compressionCodecName = CompressionCodecName.SNAPPY;

        // set Parquet file block size and page size values
        int blockSize = 256 * 1024 * 1024;
        int pageSize = 64 * 1024;
        boolean enableDictionary = true;
        int dictionaryPageSize = 64 * 1024;
        boolean validating = true;

        Path outputPath = new Path(outputFile);

        // the ParquetWriter object that will consume Avro GenericRecords
        ParquetWriter parquetWriter =
                new ParquetWriter(outputPath, writeSupport, compressionCodecName, blockSize, pageSize, dictionaryPageSize, enableDictionary, validating, conf);

        DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(avroSchema);
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(avroFile, datumReader);

        for (GenericRecord record: dataFileReader)
        {
            parquetWriter.write(record);
        }
    }
}
