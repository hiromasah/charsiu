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

package jp.ac.u.tokyo.m.dpc.pig.udf.load;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;

import jp.ac.u.tokyo.m.data.type.TypeStringCasterPigToPigTypeByte;
import jp.ac.u.tokyo.m.dpc.pig.udf.load.mapping.DPCColumnSchema;
import jp.ac.u.tokyo.m.dpc.pig.udf.load.mapping.DPCRowDataMapping;
import jp.ac.u.tokyo.m.dpc.pig.udf.load.mapping.DPCSchemaCacheLoader;
import jp.ac.u.tokyo.m.dpc.pig.udf.load.path.DefinitionResourceLoadUtil;
import jp.ac.u.tokyo.m.dpc.pig.udf.load.path.FileStatusWithVersion;
import jp.ac.u.tokyo.m.dpc.pig.udf.load.path.LoadFileFilter;
import jp.ac.u.tokyo.m.dpc.pig.udf.load.path.LoadFilesFormatParseUtil;
import jp.ac.u.tokyo.m.dpc.pig.udf.load.path.MultiFileInputFormat;
import jp.ac.u.tokyo.m.dpc.pig.udf.load.path.PathConstants;
import jp.ac.u.tokyo.m.dpc.pig.udf.load.path.PathUtil;
import jp.ac.u.tokyo.m.dpc.specific.SpecificConstants;
import jp.ac.u.tokyo.m.ini.Ini;
import jp.ac.u.tokyo.m.ini.Ini.Section;
import jp.ac.u.tokyo.m.log.LogUtil;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3native.NativeS3FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.pig.Expression;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.PigException;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.backend.hadoop.executionengine.util.MapRedUtil;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.impl.util.UDFContext;

/**
 * Reads and absorbs the difference between the file format and the schema by year of DPC data.<br>
 * <br>
 * HDFS上に特定構造で格納されている DPCデータ を、スキーマ情報付きで読み込みます。<br>
 */
public class LoadDPC extends LoadFunc implements LoadMetadata {

	// -----------------------------------------------------------------------------------------------------------------

	private static Log mLog = LogFactory.getLog(LoadDPC.class);

	// -----------------------------------------------------------------------------------------------------------------

	private String mDelimiter = "\t";
	private String mLoadTargetFileCategory;
	private String mMasterSchemaYear;

	@SuppressWarnings("rawtypes")
	private RecordReader mInputRecordReader = null;
	private PigSplit mPigSplit;
	private int mCurrentIndex = 0;

	private TupleFactory mTupleFactory = TupleFactory.getInstance();

	private DPCRowDataMapping mRowDataMapping;

	private Path mReadingFilePath = null;

	// -----------------------------------------------------------------------------------------------------------------

	public LoadDPC(String aLoadTargetFileCategory, String aMasterSchemaYear) {
		mLoadTargetFileCategory = aLoadTargetFileCategory;
		mMasterSchemaYear = aMasterSchemaYear;
	}

	public LoadDPC(String aLoadTargetFileCategory, String aMasterSchemaYear, String aDelimiter) {
		this(aLoadTargetFileCategory, aMasterSchemaYear);
		mDelimiter = aDelimiter;
	}

	// -----------------------------------------------------------------------------------------------------------------

	@Override
	public Tuple getNext() throws IOException {
		try {
			// termination | 終端検出
			boolean tNotDone = mInputRecordReader.nextKeyValue();
			if (!tNotDone) {
				return null;
			}
			return mTupleFactory.newTupleNoCopy(
					mRowDataMapping.getColumns(
							((Text) mInputRecordReader.getCurrentValue()).toString().split(mDelimiter)));
		} catch (Exception e) {
			throw new ExecException("Error while reading file \"" + mReadingFilePath.toUri().getPath() + "\""
					, 6018, PigException.REMOTE_ENVIRONMENT, e);
		}
	}

	// -----------------------------------------------------------------------------------------------------------------

	@Override
	public void prepareToRead(@SuppressWarnings("rawtypes") RecordReader aInputRecordReader, PigSplit aPigSplit) throws IOException {
		mInputRecordReader = aInputRecordReader;
		if (mPigSplit != aPigSplit)
			mCurrentIndex = 0;
		initRowDataMapping(mPigSplit = aPigSplit, mCurrentIndex++);
	}

	private void initRowDataMapping(PigSplit aPigSplit, int aSplitIndex) throws IOException {
		if (aSplitIndex >= aPigSplit.getNumPaths())
			return;
		InputSplit tSplit = aPigSplit.getWrappedSplit(aSplitIndex);
		if (tSplit instanceof FileSplit) {
			mRowDataMapping = DPCSchemaCacheLoader.INSTANCE.getRowDataMapping(mLoadTargetFileCategory, mMasterSchemaYear, (mReadingFilePath = ((FileSplit) tSplit).getPath()));
		} else
			throw new RuntimeException("InputSplit can't cast FileSplit");
	}

	// -----------------------------------------------------------------------------------------------------------------

	@SuppressWarnings("rawtypes")
	@Override
	public InputFormat getInputFormat() throws IOException {
		return new MultiFileInputFormat();
	}

	/**
	 * This method analyzes aLocation (LoadFilesFormat) and sets a target file list for a job. <br>
	 * <br>
	 * aLocation に渡された LoadFilesFormat を解析して対象となる実際のファイルをジョブに設定する。<br>
	 * 
	 * @param aLocation
	 *            "load 'xxxx';" の xxxx の部分が渡される。
	 */
	@SuppressWarnings("resource")
	@Override
	public void setLocation(String aLocation, Job aJob) throws IOException {
		Configuration tConfiguration = aJob.getConfiguration();
		// FileSystem tFileSystem = FileSystem.get(tConfiguration);
		NativeS3FileSystem tNS3FS = new NativeS3FileSystem();
		Properties tProperties = UDFContext.getUDFContext().getUDFProperties(this.getClass());
		String tS3PathTargetRoot = tProperties.getProperty(SpecificConstants.CONFIGURATION_KEY_DPC_DATA_S3_PATH
				, SpecificConstants.DPC_DATA_S3_PATH_DEFAULT);
		try {
			tNS3FS.initialize(new URI(tS3PathTargetRoot), tConfiguration);
		} catch (URISyntaxException e) {
			throw new RuntimeException(e);
		}
		FileSystem tFileSystem = tNS3FS;

		List<FileStatus> tUnfilteredLoadTargetFileStatusList = getUnfilteredLoadTargetFileStatusList(aLocation, tConfiguration, tFileSystem);
		LoadFileFilter tLoadFileFileter = createLoadFileFilter();
		LinkedHashMap<String, FileStatusWithVersion> tFileStatusVersionListContainsSubmitNumber = new LinkedHashMap<String, FileStatusWithVersion>();
		List<FileStatus> tFileStatusListNoSubmitNumber = new ArrayList<FileStatus>();
		tLoadFileFileter.filterSubmitNumber(tUnfilteredLoadTargetFileStatusList, tFileStatusVersionListContainsSubmitNumber, tFileStatusListNoSubmitNumber);

		int tFileStatusListNoSubmitNumberSize = tFileStatusListNoSubmitNumber.size();
		int tInputPathCount = tFileStatusVersionListContainsSubmitNumber.size() + tFileStatusListNoSubmitNumberSize;
		if (tInputPathCount == 0) {
			throw new IllegalArgumentException("input paths is 0. parse from the load target '" + aLocation + "'");
		}
		int tInputPathIndex = 0;
		Path[] tInputPaths = new Path[tInputPathCount];
		for (FileStatusWithVersion tFileStatusWithVersion : tFileStatusVersionListContainsSubmitNumber.values()) {
			tInputPaths[tInputPathIndex++] = tFileStatusWithVersion.getFileStatus().getPath();
		}
		for (int tIndex = 0; tIndex < tFileStatusListNoSubmitNumberSize;) {
			tInputPaths[tInputPathIndex++] = tFileStatusListNoSubmitNumber.get(tIndex++).getPath();
		}
		FileInputFormat.setInputPaths(aJob, tInputPaths);
	}

	/**
	 * @param aLocation
	 *            LoadFilesFormat '2004-2010' など
	 * @return
	 *         All FileStatus included in aLocation.<br>
	 *         aLocation に含まれる全ての FileStatus<br>
	 */
	private List<FileStatus> getUnfilteredLoadTargetFileStatusList(String aLocation, Configuration tConfiguration, FileSystem tFileSystem) throws IOException {
		Collection<String> tBaseDirectories = LoadFilesFormatParseUtil.parseLoadFilesFormat(aLocation, tConfiguration);
		ArrayList<FileStatus> tInputFileStatusList = new ArrayList<FileStatus>();
		Iterator<String> tBaseDirectoriesIterator = tBaseDirectories.iterator();
		while (tBaseDirectoriesIterator.hasNext()) {
			String tCurrentBaseDirectory = tBaseDirectoriesIterator.next();
			try {
				FileStatus tCurrentBaseDirectoryFileStatus = tFileSystem.getFileStatus(new Path(tCurrentBaseDirectory));
				tInputFileStatusList.add(tCurrentBaseDirectoryFileStatus);
			} catch (Exception e) {
				mLog.warn("dir \"" + tCurrentBaseDirectory + "\" does not exist");
				continue;
			}
		}
		List<FileStatus> tFileStatusRecursivelyList = MapRedUtil.getAllFileRecursively(tInputFileStatusList, tConfiguration);
		return tFileStatusRecursivelyList;
	}

	/**
	 * @return
	 *         Filter of FileStatus by the current setting<br>
	 *         現在の設定での FileStatus のフィルタ<br>
	 */
	private LoadFileFilter createLoadFileFilter() throws IOException {
		List<String> tLoadFilePatternStrings = DefinitionResourceLoadUtil.loadResource(PathUtil.createDefinitionFilesPath(mLoadTargetFileCategory));
		LogUtil.infoMultiParam(mLog, "load file pattern : ", "", tLoadFilePatternStrings);
		LinkedHashMap<String, String> tReplaceWords = getReplaceWords();
		return new LoadFileFilter(tLoadFilePatternStrings, tReplaceWords);
	}

	/**
	 * @return
	 *         List of substituted words defined by ".files"<br>
	 *         .files に定義されるファイル名中の置換ワードの置換対象<br>
	 */
	private LinkedHashMap<String, String> getReplaceWords() {
		LinkedHashMap<String, String> tReplaceWords = new LinkedHashMap<String, String>();
		Ini tLoadTermIni = PathConstants.INI_LOAD_TERM;
		StringBuilder tLogTextBuilder = new StringBuilder();
		tLogTextBuilder.append("load term replace : ");
		for (Entry<String, Section> tCurrentSectionEntry : tLoadTermIni.getSections().entrySet()) {
			Section tCurrentSection = tCurrentSectionEntry.getValue();
			for (Entry<String, String> tCurrentSectionMember : tCurrentSection.getMembers().entrySet()) {
				tReplaceWords.put(tCurrentSectionMember.getKey(), tCurrentSectionMember.getValue());
				tLogTextBuilder.append(tCurrentSectionMember.getKey());
				tLogTextBuilder.append("=");
				tLogTextBuilder.append(tCurrentSectionMember.getValue());
				tLogTextBuilder.append(", ");
			}
		}
		mLog.info(tLogTextBuilder.toString());
		return tReplaceWords;
	}

	/**
	 * disable a function that generate full path<br>
	 * フルパスに直す機能を潰している。<br>
	 */
	@Override
	public String relativeToAbsolutePath(String aLocation, Path tCurentDirectory) {
		return aLocation;
	}

	// -----------------------------------------------------------------------------------------------------------------

	@Override
	public ResourceSchema getSchema(String aLocation, Job aJob) throws IOException {
		ResourceSchema tResourceSchema = new ResourceSchema();
		TypeStringCasterPigToPigTypeByte tTypeCaster = TypeStringCasterPigToPigTypeByte.INSTANCE;
		List<DPCColumnSchema> tMasterSchemaColumnSchemaList =
				DPCSchemaCacheLoader.INSTANCE.getMasterSchema(mLoadTargetFileCategory, mMasterSchemaYear).getColumnSchemaList();
		int tSize = tMasterSchemaColumnSchemaList.size();
		ResourceFieldSchema[] tResourceFieldSchemas = new ResourceFieldSchema[tSize];
		for (int tIndex = 0; tIndex < tSize;) {
			DPCColumnSchema tCurrentDPCColumnSchema = tMasterSchemaColumnSchemaList.get(tIndex);
			tResourceFieldSchemas[tIndex++] =
					new ResourceFieldSchema(new FieldSchema(tCurrentDPCColumnSchema.getName()
							, tTypeCaster.castTypeString(tCurrentDPCColumnSchema.getType())));
		}
		tResourceSchema.setFields(tResourceFieldSchemas);
		return tResourceSchema;
	}

	@Override
	public String[] getPartitionKeys(String aLocation, Job aJob) throws IOException {
		return null;
	}

	@Override
	public ResourceStatistics getStatistics(String aLocation, Job aJob) throws IOException {
		return null;
	}

	@Override
	public void setPartitionFilter(Expression aPartitionFilter) throws IOException {}

	// -----------------------------------------------------------------------------------------------------------------

}
