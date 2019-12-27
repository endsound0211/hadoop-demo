package com.example.hadoopdemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.ipc.RemoteException;
import org.junit.jupiter.api.*;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.stream.Stream;

@SpringBootTest
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class HadoopDemoApplicationTests {
	private static final String HDFS_PATH = "hdfs://192.168.1.53:8020";
	private static final String HDFS_USER = "hdfs";
	private static FileSystem fileSystem;

	@BeforeAll
	public void setUp() throws URISyntaxException, IOException, InterruptedException {
		Configuration configuration = new Configuration();
		fileSystem = FileSystem.get(new URI(HDFS_PATH), configuration, HDFS_USER);
	}

	@Test
	@Order(11)
	public void createDirTest() throws IOException {
		Boolean r = fileSystem.mkdirs(new Path("/hdfs-api"));
		Assertions.assertEquals(true, r);
	}

	@Test
	@Order(12)
	public void createDuplicatedDirTest() throws IOException {
		// 創建重複路徑的dir不會有任何exception
		Boolean r = fileSystem.mkdirs(new Path("/hdfs-api"));
		Assertions.assertEquals(true, r);
	}

	@Test
	@Order(13)
	public void createFileTest() throws Exception {
		FSDataOutputStream out = fileSystem.create(new Path("/hdfs-api/test/a.txt"), true, 4096);
		out.write("A!".getBytes());
		out.flush();
		out.close();

		out = fileSystem.create(new Path("/hdfs-api/test/b.txt"),
				true, 4096);
		out.write("B!".getBytes());
		out.flush();
		out.close();
	}

	@Test
	@Order(14)
	public void createDuplicatedFileTest() throws Exception {
		Exception exception = Assertions.assertThrows(FileAlreadyExistsException.class,
				() -> fileSystem.create(new Path("/hdfs-api/test/a.txt"), false, 4096));
		String expectedMessage = "already exists";
		String actualMessage = exception.getMessage();

		Assertions.assertTrue(actualMessage.contains(expectedMessage));
	}

	@Test
	@Order(21)
	public void readNonExistFileTest() throws IOException {
		Exception exception = Assertions.assertThrows(FileNotFoundException.class,
				() -> fileSystem.open(new Path("/hdfs-api/test/c.txt")));
		String expectedMessage = "File does not exist";
		String actualMessage = exception.getMessage();

		Assertions.assertTrue(actualMessage.contains(expectedMessage));
	}

	@Test
	@Order(22)
	public void readFileTest() throws IOException{
		FSDataInputStream is = fileSystem.open(new Path("/hdfs-api/test/a.txt"));
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		byte[] buffer = new byte[4];
		int offset = 0;
		int length;
		while (is.available() > 0){
			length = is.read(buffer);
			bos.write(buffer, offset, length);
		}
		String content = new String(bos.toByteArray(),"UTF-8");
		is.close();
		bos.close();
		Assertions.assertEquals("A!", content);

	}

	@Test
	@Order(31)
	public void copyFromLocalTest() throws IOException {
		File file = new File(getClass().getClassLoader().getResource("c.txt").getFile());
		fileSystem.copyFromLocalFile(new Path(file.getAbsolutePath()), new Path("/hdfs-api/test/c.txt"));
	}

	@Test
	@Order(41)
	public void lsTest() throws IOException {
		FileStatus[] statuses = fileSystem.listStatus(new Path("/hdfs-api/test"));
		Assertions.assertEquals(3, statuses.length);
	}

	@Test
	@Order(51)
	public void deleteNonExistTest() throws IOException {
		// 刪除不存在的位址會回傳false
		Boolean r = fileSystem.delete(new Path("/hdfs-api/testAAA"), false);
		Assertions.assertEquals(false, r);
	}

	@Test()
	@Order(52)
	public void deleteDirNonEmptyTest() throws IOException {
		Exception exception = Assertions.assertThrows(RemoteException.class, () ->
				fileSystem.delete(new Path("/hdfs-api/test"), false));
		String expectedMessage = "Directory is not empty";
		String actualMessage = exception.getMessage();

		Assertions.assertTrue(actualMessage.contains(expectedMessage));
	}

	@Test()
	@Order(53)
	public void deleteDirTest() throws IOException {
		Boolean r = fileSystem.delete(new Path("/hdfs-api"), true);
		Assertions.assertEquals(true, r);
	}

	@AfterAll
	public void tearDown() {
		fileSystem = null;
	}
}
