package com.github.ashleytaylor.thumbnailer;

import java.io.IOException;
import java.util.Map;

import com.hazelcast.core.Immutable;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;

public record ImageWrapper(byte[] raw, Map<Integer, Thumbnail> thumbnails) implements Immutable {

	static class Serializer implements StreamSerializer<ImageWrapper> {

		@Override
		public int getTypeId() {
			return 1;
		}

		@Override
		public void write(ObjectDataOutput out, ImageWrapper object) throws IOException {
			out.writeByteArray(object.raw());
			out.writeObject(object.thumbnails());

		}

		@Override
		public ImageWrapper read(ObjectDataInput in) throws IOException {
			return new ImageWrapper(in.readByteArray(), in.readObject());
		}
	}
}
