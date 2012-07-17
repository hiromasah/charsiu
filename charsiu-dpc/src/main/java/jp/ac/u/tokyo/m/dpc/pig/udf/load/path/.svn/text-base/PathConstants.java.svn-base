package jp.ac.u.tokyo.m.dpc.pig.udf.load.path;

import jp.ac.u.tokyo.m.ini.Ini;
import jp.ac.u.tokyo.m.resource.ResourceLoadUtil;

public class PathConstants {

	// -----------------------------------------------------------------------------------------------------------------

	public static final String HOSPITAL_CODE = "HOSPITAL_CODE";
	public static final String yyyyMM = "yyyyMM";
	public static final String GENERATE_NUMBER = "GENERATE_NUMBER";
	public static final String SUBMIT_NUMBER = "SUBMIT_NUMBER";

	// -----------------------------------------------------------------------------------------------------------------

	private static final String RESOURCE_PATH_DEFINITION_DIRECTORY = "definition/";
	public static final String RESOURCE_PATH_DEFINITION_FILES_DIRECTORY = RESOURCE_PATH_DEFINITION_DIRECTORY + "files/";
	private static final String RESOURCE_PATH_DEFINITION_TERM_DIRECTORY = RESOURCE_PATH_DEFINITION_DIRECTORY + "term/";
	public static final String RESOURCE_PATH_DEFINITION_SCHEMA_DIRECTORY = RESOURCE_PATH_DEFINITION_DIRECTORY + "schema/";

	public static final String RESOURCE_PART_DEFINITION_SCHEMA_DIRECTORY_TEXT = "/text/";

	public static final String EXTENSION_DEFINITION_FILES = ".files";
	public static final String EXTENSION_DEFINITION_SCHEMA = ".schema";
	public static final String EXTENSION_INI = ".ini";
	public static final String EXTENSION_TEXT = ".txt";
	public static final String EXTENSION_SEQUENCE = ".seq";

	private static final String RESOURCE_FILE_NAME_DEFINITION_LOAD_TERM = "load-term" + EXTENSION_INI;
	private static final String RESOURCE_PATH_DEFINITION_LOAD_TERM_FILE = RESOURCE_PATH_DEFINITION_TERM_DIRECTORY + RESOURCE_FILE_NAME_DEFINITION_LOAD_TERM;

	// -----------------------------------------------------------------------------------------------------------------

	public static final String INI_SECTION_RESERVED_WORD = "reserved-word";

	// -----------------------------------------------------------------------------------------------------------------

	public static final Ini INI_LOAD_TERM = ResourceLoadUtil.loadNecessaryPublicIni(
			new PathConstants().getClass(), RESOURCE_PATH_DEFINITION_LOAD_TERM_FILE);

	// -----------------------------------------------------------------------------------------------------------------

}
