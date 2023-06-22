package com.example.demo.remote.chunk;

import org.apache.kafka.common.serialization.Serializer;
import org.springframework.batch.integration.chunk.ChunkResponse;
import org.springframework.util.SerializationUtils;

public class ChunkResponseSerializer implements Serializer<ChunkResponse> {

	@Override
	public byte[] serialize(String topic, ChunkResponse data) {
		if (data == null) {
			return new byte[0];
		}
		return SerializationUtils.serialize(data);
	}

}
