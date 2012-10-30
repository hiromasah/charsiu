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

package jp.ac.u.tokyo.m.dpc.pig.udf.load.mapping;

import java.util.ArrayList;
import java.util.List;

public class DPCRowDataMapping {

	private final ResultColumn[] mRowIndexMappingArray;

	public DPCRowDataMapping(DPCSchema aMasterSchema, DPCSchema aReadSchema) {
		// create index-mapping
		TypeStringCasterPigToColumnDataCaster tTypeCaster = TypeStringCasterPigToColumnDataCaster.INSTANCE;
		List<DPCColumnSchema> tMasterSchemaColumnSchemaList = aMasterSchema.getColumnSchemaList();
		int tSize = tMasterSchemaColumnSchemaList.size();
		ResultColumn[] tRowIndexMappingArray = mRowIndexMappingArray = new ResultColumn[tSize];
		List<DPCColumnSchema> tReadSchemaColumnSchemaList = aReadSchema.getColumnSchemaList();
		for (int tIndex = 0; tIndex < tSize;) {
			DPCColumnSchema tCurrentDPCColumnSchema = tMasterSchemaColumnSchemaList.get(tIndex);
			String tCurrentMasterSchemaType = tCurrentDPCColumnSchema.getType();
			int tReadDataIndex = tReadSchemaColumnSchemaList.indexOf(tCurrentDPCColumnSchema);
			if (tReadDataIndex >= 0) {
				tRowIndexMappingArray[tIndex++] = new IndexColumn(tReadDataIndex,
						tTypeCaster.castTypeString(tCurrentMasterSchemaType));
			} else {
				tRowIndexMappingArray[tIndex++] = NullColumn.INSTANCE;
			}
		}
	}

	public List<Object> getColumns(String[] aValues) {
		ArrayList<Object> tColumns = new ArrayList<Object>();
		ResultColumn[] tRowIndexMappingArray = mRowIndexMappingArray;
		int tRowIndexMappingArrayLength = tRowIndexMappingArray.length;
		for (int tIndex = 0; tIndex < tRowIndexMappingArrayLength;) {
			tColumns.add(tRowIndexMappingArray[tIndex++].getValue(aValues));
		}
		return tColumns;
	}

}
