package jp.ac.u.tokyo.m.dpc.pig.udf.load.mapping;

import java.util.ArrayList;
import java.util.List;

public class DPCRowDataMapping {

	private final ResultColumn[] mRowIndexMappingArray;

	public DPCRowDataMapping(DPCSchema aMasterSchema, DPCSchema aReadSchema) {
		// index-mapping 作成
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
