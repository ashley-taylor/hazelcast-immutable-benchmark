package com.github.ashleytaylor.thumbnailer;

import java.io.IOException;

import com.hazelcast.core.Immutable;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;

public record Thumbnail(byte[] image) implements Immutable {
	static class Serializer implements StreamSerializer<Thumbnail> {

		@Override
		public int getTypeId() {
			return 2;
		}

		@Override
		public void write(ObjectDataOutput out, Thumbnail object) throws IOException {
			out.writeByteArray(object.image());
		}

		@Override
		public Thumbnail read(ObjectDataInput in) throws IOException {
			return new Thumbnail(in.readByteArray());
		}
	}
}
