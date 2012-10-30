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

package jp.ac.u.tokyo.m.pig.udf.store;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;

import jp.ac.u.tokyo.m.pig.udf.StoreConstants;
import jp.ac.u.tokyo.m.string.StringFormatConstants;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.StoreMetadata;
import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.backend.datastorage.ElementDescriptor;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.backend.hadoop.datastorage.HDataStorage;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.DataType;

/**
 * StoreDataWithSchema outputs alias with data. <br>
 * <br>
 * データと共にエイリアスを出力します。<br>
 */
public class StoreDataWithSchema extends PigStorage implements StoreMetadata {

	// -----------------------------------------------------------------------------------------------------------------

	private String mFieldDelimiter = StoreConstants.STORE_DELIMITER_FIELD;

	// -----------------------------------------------------------------------------------------------------------------

	public StoreDataWithSchema() {
		super();
	}

	public StoreDataWithSchema(String aFieldDelimiter) {
		super(aFieldDelimiter);
		mFieldDelimiter = aFieldDelimiter;
	}

	// -----------------------------------------------------------------------------------------------------------------
	// Implementation of StoreMetadata

	@Override
	public void storeSchema(ResourceSchema aSchema, String aLocation, Job aJob) throws IOException {
		// outputs .header and .schema | .header と .schema を出力する
		Configuration tConfiguration = aJob.getConfiguration();
		DataStorage tStorage = new HDataStorage(ConfigurationUtil.toProperties(tConfiguration));
		ElementDescriptor tHeaderFilePath = tStorage.asElement(aLocation, StoreConstants.STORE_FILE_NAME_HEADER);
		ElementDescriptor tSchemaFilePath = tStorage.asElement(aLocation, StoreConstants.STORE_FILE_NAME_SCHEMA);
		OutputStream tHeaderOutputStream = tHeaderFilePath.create();
		OutputStream tSchemaOutputStream = tSchemaFilePath.create();
		String tFieldDelimiter = mFieldDelimiter;
		String tRecordDelimiter = StoreConstants.STORE_DELIMITER_RECORD;
		try {
			// outputs name of 1st layer of aSchema | aSchema の1層目の name を取得して出力する。
			ResourceFieldSchema[] tFields = aSchema.getFields();
			int tFieldLength = tFields.length;
			if (tFieldLength > 0) {
				String tCurrentFieldName = tFields[0].getName();
				writeFieldName(tHeaderOutputStream, tCurrentFieldName, 0);
				writeSchema(tSchemaOutputStream, tCurrentFieldName, tFields[0].getType(), 0, tRecordDelimiter);
				for (int tFieldIndex = 1; tFieldIndex < tFieldLength; tFieldIndex++) {
					tCurrentFieldName = tFields[tFieldIndex].getName();
					writeString(tHeaderOutputStream, tFieldDelimiter);
					writeFieldName(tHeaderOutputStream, tCurrentFieldName, tFieldIndex);
					writeSchema(tSchemaOutputStream, tCurrentFieldName, tFields[tFieldIndex].getType(), tFieldIndex, tRecordDelimiter);
				}
				writeString(tHeaderOutputStream, tRecordDelimiter);
			}
		} finally {
			tHeaderOutputStream.close();
			tSchemaOutputStream.close();
		}
	}

	private void writeSchema(OutputStream aSchemaOutputStream, String aCurrentFieldName, byte aCurrentFieldType, int aIndex, String aRecordDelimiter)
			throws IOException, UnsupportedEncodingException {
		writeFieldName(aSchemaOutputStream, aCurrentFieldName, aIndex);
		writeString(aSchemaOutputStream, StoreConstants.STORE_SCHEMA_ALIAS_TYPE_SEPARATOR);
		writeString(aSchemaOutputStream, DataType.findTypeName(aCurrentFieldType));
		writeString(aSchemaOutputStream, aRecordDelimiter);
	}

	private void writeFieldName(OutputStream aOutputStream, String aFieldName, int aIndex) throws IOException, UnsupportedEncodingException {
		if (aFieldName == null || aFieldName.length() == 0) {
			writeString(aOutputStream, StoreConstants.STORE_DEFAULT_COLUMN_NAME + aIndex);
		} else {
			writeString(aOutputStream, aFieldName);
		}
	}

	private void writeString(OutputStream aOutputStream, String aFieldName) throws IOException, UnsupportedEncodingException {
		aOutputStream.write(aFieldName.getBytes(StringFormatConstants.TEXT_FORMAT_UTF8));
	}

	// -----------------------------------------------------------------------------------------------------------------
	// Implementation of StoreMetadata

	@Override
	public void storeStatistics(ResourceStatistics aStats, String aLocation, Job aJob) throws IOException {}

	// -----------------------------------------------------------------------------------------------------------------

}
