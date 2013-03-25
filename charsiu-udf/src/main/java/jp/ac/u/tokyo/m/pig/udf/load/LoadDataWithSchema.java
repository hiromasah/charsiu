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

package jp.ac.u.tokyo.m.pig.udf.load;

import java.io.IOException;
import java.util.List;

import jp.ac.u.tokyo.m.data.type.TypeStringCasterPigToPigTypeByte;
import jp.ac.u.tokyo.m.pig.udf.StoreConstants;
import jp.ac.u.tokyo.m.string.StringFormatConstants;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.Expression;
import org.apache.pig.LoadMetadata;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

/**
 * Load data with schema information. <br>
 * <br>
 * データをスキーマ情報付きで読み込みます。<br>
 */
public class LoadDataWithSchema extends PigStorage implements LoadMetadata {

	// -----------------------------------------------------------------------------------------------------------------

	private String mEncoding = StringFormatConstants.TEXT_FORMAT_UTF8;

	// private String mSchemaFileLocation = null;

	// -----------------------------------------------------------------------------------------------------------------

	public LoadDataWithSchema() {
		super();
	}

	public LoadDataWithSchema(String aDelimiter) {
		super(aDelimiter);
	}

	// TODO 実装
	// public LoadDataWithSchema(String aDelimiter, String aEncoding) {
	// this(aDelimiter);
	// mEncoding = aEncoding;
	// }

	// TODO 実装
	// public LoadDataWithSchema(String aDelimiter, String aEncoding, String aSchemaFileLocation) {
	// this(aDelimiter, aEncoding);
	// mSchemaFileLocation = aSchemaFileLocation;
	// }

	// -----------------------------------------------------------------------------------------------------------------

	// TODO 実装
	// @SuppressWarnings("rawtypes")
	// @Override
	// public InputFormat getInputFormat() throws IOException {
	// return new FreeEncodingPigTextInputFormat(mFieldDelimiter, mEncoding);
	// }

	// -----------------------------------------------------------------------------------------------------------------
	// Implementation of LoadMetadata

	// XXX store, dump. describe など1回しか行わないならば1回呼出のはずだが、一応キャッシュできる方が効率は良いかもしれない
	@Override
	public ResourceSchema getSchema(String aLocation, Job aJob) throws IOException {
		Configuration tConfiguration = aJob.getConfiguration();
		Path tDataPath = new Path(aLocation);
		FileSystem tFileSystem = tDataPath.getFileSystem(tConfiguration);
		Path tSchemaFilePath = tFileSystem.isFile(tDataPath)
				? new Path(tDataPath.getParent(), StoreConstants.STORE_FILE_NAME_SCHEMA)
				: new Path(tDataPath, StoreConstants.STORE_FILE_NAME_SCHEMA);
		RowSchema tRowSchema = LoadSchemaUtil.loadSchemaFile(tFileSystem, tSchemaFilePath, mEncoding);

		ResourceSchema tResourceSchema = new ResourceSchema();
		TypeStringCasterPigToPigTypeByte tTypeCaster = TypeStringCasterPigToPigTypeByte.INSTANCE;
		List<ColumnSchema> tColumnSchemaList = tRowSchema.getColumnSchemaList();
		int tSize = tColumnSchemaList.size();
		ResourceFieldSchema[] tResourceFieldSchemas = new ResourceFieldSchema[tSize];
		int tIndex = 0;
		for (ColumnSchema tCurrentColumnSchema : tColumnSchemaList) {
			tResourceFieldSchemas[tIndex++] =
					new ResourceFieldSchema(new FieldSchema(tCurrentColumnSchema.getName()
							, tTypeCaster.castTypeString(tCurrentColumnSchema.getType())));
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
