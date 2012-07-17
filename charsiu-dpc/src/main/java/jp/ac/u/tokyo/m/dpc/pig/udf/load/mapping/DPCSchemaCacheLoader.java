package jp.ac.u.tokyo.m.dpc.pig.udf.load.mapping;

import java.io.IOException;
import java.util.HashMap;

import jp.ac.u.tokyo.m.dpc.pig.udf.load.path.PathUtil;
import jp.ac.u.tokyo.m.dpc.pig.udf.load.path.DefinitionResourceLoadUtil;

import org.apache.hadoop.fs.Path;

public class DPCSchemaCacheLoader {

	// -----------------------------------------------------------------------------------------------------------------

	public static final DPCSchemaCacheLoader INSTANCE = new DPCSchemaCacheLoader();

	private DPCSchemaCacheLoader() {};

	// -----------------------------------------------------------------------------------------------------------------

	private final HashMap<String, DPCSchema> mSchemaCache = new HashMap<String, DPCSchema>();

	private final HashMap<String, DPCRowDataMapping> mRowDataMappingCache = new HashMap<String, DPCRowDataMapping>();

	// -----------------------------------------------------------------------------------------------------------------

	public DPCSchema getMasterSchema(String aFileCategory, String aMasterSchemaYear) throws IOException {
		String tMasterSchemaFilePath = PathUtil.getSchemaFilePath(aMasterSchemaYear, aFileCategory);
		DPCSchema tResultMasterSchema = mSchemaCache.get(tMasterSchemaFilePath);
		if (tResultMasterSchema == null) {
			mSchemaCache.put(tMasterSchemaFilePath, tResultMasterSchema = DefinitionResourceLoadUtil.loadSchemaResource(tMasterSchemaFilePath));
			return tResultMasterSchema;
		} else {
			return tResultMasterSchema;
		}
	}

	public DPCSchema getReadSchema(String aFileCategory, Path aReadSchemaFilePath) throws IOException {
		String tReadSchemaFilePath = PathUtil.getReadSchemaFilePath(aReadSchemaFilePath, aFileCategory);
		DPCSchema tResultReadSchema = mSchemaCache.get(tReadSchemaFilePath);
		if (tResultReadSchema == null) {
			mSchemaCache.put(tReadSchemaFilePath, tResultReadSchema = DefinitionResourceLoadUtil.loadSchemaResource(tReadSchemaFilePath));
			return tResultReadSchema;
		} else {
			return tResultReadSchema;
		}
	}

	// -----------------------------------------------------------------------------------------------------------------

	public DPCRowDataMapping getRowDataMapping(String aFileCategory, String aMasterSchemaYear, Path aReadSchemaFilePath) throws IOException {
		String tMasterSchemaFilePath = PathUtil.getSchemaFilePath(aMasterSchemaYear, aFileCategory);
		String tReadSchemaFilePath = PathUtil.getReadSchemaFilePath(aReadSchemaFilePath, aFileCategory);
		String tKey = tMasterSchemaFilePath + tReadSchemaFilePath;
		DPCRowDataMapping tResultDpcRowDataMapping = mRowDataMappingCache.get(tKey);
		if (tResultDpcRowDataMapping == null) {
			DPCSchema tMasterSchema = getMasterSchema(aFileCategory, aMasterSchemaYear);
			DPCSchema tReadSchema = getReadSchema(aFileCategory, aReadSchemaFilePath);
			mRowDataMappingCache.put(tKey, tResultDpcRowDataMapping = new DPCRowDataMapping(tMasterSchema, tReadSchema));
			return tResultDpcRowDataMapping;
		} else {
			return tResultDpcRowDataMapping;
		}
	}

	// -----------------------------------------------------------------------------------------------------------------

}
