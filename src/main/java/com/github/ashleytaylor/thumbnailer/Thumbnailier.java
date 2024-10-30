package com.github.ashleytaylor.thumbnailer;

import java.awt.AlphaComposite;
import java.awt.Graphics2D;
import java.awt.Image;
import java.awt.RenderingHints;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import javax.imageio.ImageIO;

import com.google.common.io.ByteStreams;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.Offloadable;
import com.hazelcast.core.ReadOnly;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;

public class Thumbnailier implements EntryProcessor<String, ImageWrapper, Thumbnail>, Offloadable, ReadOnly, HazelcastInstanceAware {

	private final String imagesMapName;
	private final int size;

	private transient HazelcastInstance hazelcastInstance;

	public Thumbnailier(String imagesMapName, int size) {
		this.imagesMapName = imagesMapName;
		this.size = size;
	}

	@Override
	public Thumbnail process(Entry<String, ImageWrapper> entry) {
		var value = entry.getValue();
		if (value == null) {
			value = load(entry.getKey());
		}

		var thumbnail = value.thumbnails().get(size);
		if (thumbnail == null) {
			try {
				thumbnail = resize(value.raw(), size);
			} catch (IOException e) {
				throw new UncheckedIOException(e);
			}
			var map = new HashMap<>(value.thumbnails());
			map.put(size, thumbnail);
			value = new ImageWrapper(value.raw(), map);
			// make
			hazelcastInstance.getMap(imagesMapName).put(entry.getKey(), value);
		}

		return thumbnail;
	}

	private Thumbnail resize(byte[] raw, int newWidth) throws IOException {
		var image = ImageIO.read(new ByteArrayInputStream(raw));

		var newHeight = size * image.getWidth() / image.getHeight();

		Image originalImage = image.getScaledInstance(newWidth, newHeight, Image.SCALE_DEFAULT);

		int type = ((image.getType() == 0) ? BufferedImage.TYPE_INT_ARGB : image.getType());
		BufferedImage resizedImage = new BufferedImage(newWidth, newHeight, type);

		Graphics2D g2d = resizedImage.createGraphics();
		g2d.drawImage(originalImage, 0, 0, newWidth, newHeight, null);
		g2d.dispose();
		g2d.setComposite(AlphaComposite.Src);
		g2d.setRenderingHint(RenderingHints.KEY_INTERPOLATION, RenderingHints.VALUE_INTERPOLATION_BILINEAR);
		g2d.setRenderingHint(RenderingHints.KEY_RENDERING, RenderingHints.VALUE_RENDER_QUALITY);
		g2d.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);

		ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

		ImageIO.write(resizedImage, "jpeg", byteArrayOutputStream);
		return new Thumbnail(byteArrayOutputStream.toByteArray());
	}

	private ImageWrapper load(String key) {
		try (var is = getClass().getResourceAsStream("/images/" + key)) {
			return new ImageWrapper(ByteStreams.toByteArray(is), Map.of());
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}

	}

	@Override
	public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
		this.hazelcastInstance = hazelcastInstance;

	}

	@Override
	public String getExecutorName() {
		return OFFLOADABLE_EXECUTOR;
	}

	static class Serializer implements StreamSerializer<Thumbnailier> {

		@Override
		public int getTypeId() {
			return 3;
		}

		@Override
		public void write(ObjectDataOutput out, Thumbnailier object) throws IOException {
			out.writeString(object.imagesMapName);
			out.writeInt(object.size);

		}

		@Override
		public Thumbnailier read(ObjectDataInput in) throws IOException {
			return new Thumbnailier(in.readString(), in.readInt());
		}
	}

}
