package com.example.demo.remote.partition.partitioner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.core.io.ClassPathResource;

import com.example.demo.remote.partition.constants.BatchConstants;

import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.HashMap;
import java.util.Map;

public class CsvStepPartitioner implements Partitioner {

    private static final Logger logger = LoggerFactory.getLogger(CsvStepPartitioner.class);

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        Map<String, ExecutionContext> result = new HashMap<>();

        int noOfLines = 0;
        try {
            noOfLines = getNoOfLines(BatchConstants.STUDENTS_FILENAME);
        } catch (IOException e) {
            e.printStackTrace();
        }

        int firstLine = 1;
        int partitionNumber = 0;
        int countLines = noOfLines / gridSize;

        while (partitionNumber < gridSize) {

            logger.info("Partition number : {}, first line is : {}, count  line is : {} ", partitionNumber, firstLine,
                    countLines);

            ExecutionContext value = new ExecutionContext();

            value.putLong("partition_number", partitionNumber);
            value.putLong("first_line", firstLine);
            value.putLong("item_count", countLines);

            result.put("PartitionNumber-" + partitionNumber, value);

            firstLine = firstLine + countLines;
            partitionNumber++;
        }

        logger.info("No of lines {}", noOfLines);

        return result;
    }

    public int getNoOfLines(String fileName) throws IOException {
        ClassPathResource classPathResource = new ClassPathResource(fileName);
        try (LineNumberReader reader = new LineNumberReader(
                new FileReader(classPathResource.getFile().getAbsolutePath()))) {
            reader.skip(Integer.MAX_VALUE);
            return reader.getLineNumber();
        }
    }
}
