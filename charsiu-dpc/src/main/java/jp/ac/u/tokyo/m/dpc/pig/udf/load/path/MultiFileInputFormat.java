/*
 * Copyright 2012 Hiromasa Horiguchi ( The University of Tokyo )
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

package jp.ac.u.tokyo.m.dpc.pig.udf.load.path;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class MultiFileInputFormat extends TextInputFormat {

	/**
	 * return reader which accepted extension. <br>
	 * <br>
	 * 拡張子に応じた reader を返します。 <br>
	 * SequenceFile : 「.seq」 <br>
	 * テキストファイル : 「.txt」またはそのほかの拡張子 <br>
	 */
	@Override
	public RecordReader<LongWritable, Text> createRecordReader(InputSplit aSplit, TaskAttemptContext aContext) {
		// FileSplit であればファイル名を判別する。
		if (aSplit instanceof FileSplit) {
			FileSplit tFileSplit = (FileSplit) aSplit;
			Path tPath = tFileSplit.getPath();
			String tFileName = tPath.getName();
			String tExtension = tFileName.substring(tFileName.lastIndexOf("."));
			// 拡張子に応じた RecordReader を返す。
			if (tExtension != null) {
				if (tExtension.equals(PathConstants.EXTENSION_TEXT)) {
					return new LineRecordReader();
				} else if (tExtension.equals(PathConstants.EXTENSION_SEQUENCE)) {
					return new SequenceFileRecordReader<LongWritable, Text>();
				}
			}
		}
		// デフォルトは Text の RecordReader
		return new LineRecordReader();
	}

}
