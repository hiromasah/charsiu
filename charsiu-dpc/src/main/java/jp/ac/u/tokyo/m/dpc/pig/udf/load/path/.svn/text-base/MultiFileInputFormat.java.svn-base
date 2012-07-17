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
	 * 拡張子に応じた reader を返します。
	 * SequenceFile : 「.seq」
	 * テキストファイル : 「.txt」またはそのほかの拡張子
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
