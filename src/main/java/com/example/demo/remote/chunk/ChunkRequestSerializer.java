package com.example.demo.remote.chunk;

import org.apache.kafka.common.serialization.Serializer;
import org.springframework.batch.integration.chunk.ChunkRequest;
import org.springframework.util.SerializationUtils;

public class ChunkRequestSerializer implements Serializer<ChunkRequest<TransacaoDTO>> {

	@Override
	public byte[] serialize(String topic, ChunkRequest<TransacaoDTO> data) {
		if (data == null) {
			return new byte[0];
		}
		return SerializationUtils.serialize(data);
	}

}
