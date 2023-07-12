package com.example.demo.remote.partition.partitioner;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.sshd.sftp.client.SftpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.integration.file.remote.RemoteFileTemplate;
import org.springframework.stereotype.Component;

import com.example.demo.remote.partition.constants.BatchConstants;

@Component
public class CsvStepPartitioner implements Partitioner {

    @Autowired
    private RemoteFileTemplate<SftpClient.DirEntry> remoteFileTemplate;

    private static final Logger logger = LoggerFactory.getLogger(CsvStepPartitioner.class);

    @Override
    public Map<String, ExecutionContext> partition(final int gridSize) {
        final Map<String, ExecutionContext> result = new HashMap<>();

        int noOfLines = 0;
        try {
            noOfLines = getNoOfLines();
        } catch (final IOException e) {
            e.printStackTrace();
        }

        int firstLine = 1;
        int partitionNumber = 0;
        final int countLines = noOfLines / gridSize;

        while (partitionNumber < gridSize) {

            logger.info("Partition number : {}, first line is : {}, count  line is : {} ", partitionNumber, firstLine,
                    countLines);

            final ExecutionContext value = new ExecutionContext();

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

    public int getNoOfLines() throws IOException {

        final String remoteFilePath = BatchConstants.REMOTO_FILE_PATH + BatchConstants.FILENAME;

        try (InputStream inputStream = remoteFileTemplate.execute(session -> session.readRaw(remoteFilePath))) {
            final InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
            try (LineNumberReader reader = new LineNumberReader(inputStreamReader)) {
                reader.skip(Integer.MAX_VALUE);
                return reader.getLineNumber();
            }
        }
        
    }
}
