package com.github.ashleytaylor.thumbnailer;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

public class BenchmarkThumbnailer {

	public static final String IMAGES_MAP_NAME = "images";

	public static void main(String[] args) throws Exception {
		var opt = new OptionsBuilder()
			.include(BenchmarkThumbnailer.class.getName() + ".*")
			.mode(Mode.Throughput)
			.timeUnit(TimeUnit.SECONDS)
			.warmupTime(TimeValue.seconds(1))
			.warmupIterations(4)
			.measurementTime(TimeValue.seconds(1))
			.measurementIterations(30)
			.threads(2)
			.forks(1)
			.shouldFailOnError(true)
			.shouldDoGC(true)
			.build();
		new Runner(opt).run();
	}

	@State(Scope.Benchmark)
	public static class Data {

		public HazelcastInstance hz;
		private List<String> paths;
		private IMap<String, ImageWrapper> map;
		private ThreadLocalRandom random;

		public Data() {

			var imagesPath = BenchmarkThumbnailer.class.getResource("/images");

			try {
				this.paths = Files.list(Paths.get(imagesPath.toURI())).map(p -> p.getFileName().toString()).toList();
			} catch (IOException | URISyntaxException e) {
				throw new RuntimeException(e);
			}
			this.random = ThreadLocalRandom.current();

		}

		@Setup
		public void setup(BenchmarkParams params) {

			var config = new Config();

			config
				.getSerializationConfig()
				.addSerializerConfig(new SerializerConfig().setImplementation(new ImageWrapper.Serializer()).setTypeClass(ImageWrapper.class));
			config
				.getSerializationConfig()
				.addSerializerConfig(new SerializerConfig().setImplementation(new Thumbnail.Serializer()).setTypeClass(Thumbnail.class));
			config
				.getSerializationConfig()
				.addSerializerConfig(new SerializerConfig().setImplementation(new Thumbnailier.Serializer()).setTypeClass(Thumbnailier.class));

			config.getMapConfig(IMAGES_MAP_NAME).setInMemoryFormat(InMemoryFormat.OBJECT).setBackupCount(0).setAsyncBackupCount(0);
			for (int i = 0; i < Integer.parseInt(nodes); i++) {
				this.hz = Hazelcast.newHazelcastInstance(config);
			}
			this.map = hz.getMap(IMAGES_MAP_NAME);
		}

		@TearDown
		public void stop() {
			Hazelcast.shutdownAll();
		}

		@Param({ "1", "3", "5", "7", "9" })
		public static String nodes;

	}

	@Benchmark
	public void get(Data data) {
		data.map.executeOnKey(data.paths.get(data.random.nextInt(data.paths.size())), new Thumbnailier(IMAGES_MAP_NAME, 500));
	}

}
