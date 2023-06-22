package com.example.demo.remote.chunk;

import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.batch.integration.chunk.ChunkRequest;
import org.springframework.util.SerializationUtils;

public class ChunkRequestDeserializer implements Deserializer<ChunkRequest<TransacaoDTO>> {

	@Override
	public ChunkRequest<TransacaoDTO> deserialize(String topic, byte[] data) {

		if (data == null) {
			return null;
		}

		return (ChunkRequest) SerializationUtils.deserialize(data);
	}

}
