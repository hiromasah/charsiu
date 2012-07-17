package jp.ac.u.tokyo.m.string;

import java.text.SimpleDateFormat;
import java.util.regex.Pattern;

public interface StringFormatConstants {

	public static final SimpleDateFormat FORMAT_TIMESTAMP = new SimpleDateFormat("yyyyMMddHHmmssSSS");

	public static final SimpleDateFormat FORMAT_DATE = new SimpleDateFormat("yyyyMMdd");

	public static final Pattern PATTERN_YEAR = Pattern.compile("([0-9]{4})");

	public static final String TEXT_FORMAT_UTF8 = "UTF-8";

}
