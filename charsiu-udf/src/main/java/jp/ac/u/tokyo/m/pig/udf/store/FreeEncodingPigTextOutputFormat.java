/*
 * Copyright 2012-2013 Hiromasa Horiguchi ( The University of Tokyo )
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package jp.ac.u.tokyo.m.pig.udf.store;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import jp.ac.u.tokyo.m.string.StringFormatConstants;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigTextOutputFormat;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.StorageUtil;

public class FreeEncodingPigTextOutputFormat extends PigTextOutputFormat {

	// -----------------------------------------------------------------------------------------------------------------

	private final byte mFieldDelimiter;
	private String mEncoding = StringFormatConstants.TEXT_FORMAT_UTF8;

	// -----------------------------------------------------------------------------------------------------------------

	public FreeEncodingPigTextOutputFormat(byte aDelimiter) {
		super(aDelimiter);
		mFieldDelimiter = aDelimiter;
	}

	public FreeEncodingPigTextOutputFormat(byte aDelimiter, String aEncording) {
		this(aDelimiter);
		mEncoding = aEncording;
	}

	public FreeEncodingPigTextOutputFormat(String aDelimiter, String aEncording) {
		this(StorageUtil.parseFieldDel(aDelimiter));
		mEncoding = aEncording;
	}

	// -----------------------------------------------------------------------------------------------------------------

	@SuppressWarnings("rawtypes")
	@Override
	public RecordWriter<WritableComparable, Tuple> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
		Configuration conf = job.getConfiguration();
		boolean isCompressed = getCompressOutput(job);
		CompressionCodec codec = null;
		String extension = "";
		if (isCompressed) {
			Class<? extends CompressionCodec> codecClass =
					getOutputCompressorClass(job, GzipCodec.class);
			codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
			extension = codec.getDefaultExtension();
		}
		Path file = getDefaultWorkFile(job, extension);
		FileSystem fs = file.getFileSystem(conf);
		if (!isCompressed) {
			FSDataOutputStream fileOut = fs.create(file, false);
			return new FreeEncodingPigLineRecordWriter(fileOut, mFieldDelimiter, mEncoding);
		} else {
			FSDataOutputStream fileOut = fs.create(file, false);
			return new FreeEncodingPigLineRecordWriter(new DataOutputStream
					(codec.createOutputStream(fileOut)), mFieldDelimiter, mEncoding);
		}
	}

	// -----------------------------------------------------------------------------------------------------------------

	@SuppressWarnings("rawtypes")
	protected static class FreeEncodingPigLineRecordWriter extends TextOutputFormat.LineRecordWriter<WritableComparable, Tuple> {

		private final byte mFieldDelimiter;
		private final byte[] mNewline;
		private final String mEncoding;

		public FreeEncodingPigLineRecordWriter(DataOutputStream aOut, byte aFieldDelimiter, String aEncoding) {
			super(aOut);
			this.mFieldDelimiter = aFieldDelimiter;
			try {
				mNewline = "\n".getBytes(aEncoding);
			} catch (UnsupportedEncodingException uee) {
				throw new IllegalArgumentException("can't find " + aEncoding + " encoding");
			}
			mEncoding = aEncoding;
		}

		public synchronized void write(WritableComparable key, Tuple value)
				throws IOException {
			int sz = value.size();
			for (int i = 0; i < sz; i++) {
				FreeEncodingStorageUtil.putField(out, value.get(i), mEncoding);
				if (i != sz - 1) {
					out.writeByte(mFieldDelimiter);
				}
			}
			out.write(mNewline);
		}
	}

	// -----------------------------------------------------------------------------------------------------------------

}
