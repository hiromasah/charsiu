package jp.ac.u.tokyo.m.resource;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import jp.ac.u.tokyo.m.ini.Ini;

import org.apache.commons.logging.LogFactory;

public class ResourceLoadUtil {

	// -----------------------------------------------------------------------------------------------------------------

	/**
	 * aFileName に指定する同じパッケージ内の Ini を読み込みます。
	 * 指定ファイルが存在しない場合はエラー。
	 */
	public static Ini loadNecessaryPackagePrivateIni(Class<?> aClass, String aFileName) {
		return loadNecessaryIni(aClass, aFileName, aClass.getResourceAsStream(aFileName));
	}

	/**
	 * aFileName に指定するjar内の Ini を読み込みます。
	 * 指定ファイルが存在しない場合はエラー。
	 */
	public static Ini loadNecessaryPublicIni(Class<?> aClass, String aFileName) {
		return loadNecessaryIni(aClass, aFileName, aClass.getClassLoader().getResourceAsStream(aFileName));
	}

	private static Ini loadNecessaryIni(Class<?> aClass, String aFileName, InputStream aResourceAsStream) {
		try {
			return loadIni(aClass, aFileName, aResourceAsStream);
		} catch (NullPointerException e) {
			throw new RuntimeException(new FileNotFoundException(aClass.getPackage() + "." + aFileName));
		}
	}

	/**
	 * aFileName に指定する同じパッケージ内の Ini を読み込みます。
	 * 指定ファイルが存在しない場合は警告のみ。
	 */
	public static Ini loadUnnecessaryPackagePrivateIni(Class<?> aClass, String aFileName) {
		try {
			return loadIni(aClass, aFileName, aClass.getResourceAsStream(aFileName));
		} catch (NullPointerException e) {
			LogFactory.getLog(aClass).warn("not found : " + aClass.getPackage() + "." + aFileName);
			return new Ini();
		}
	}

	private static Ini loadIni(Class<?> aClass, String aFileName, InputStream aResourceAsStream) throws NullPointerException {
		try {
			try {
				Ini tIni = new Ini();
				tIni.load(aResourceAsStream);
				return tIni;
			} finally {
				aResourceAsStream.close();
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	// -----------------------------------------------------------------------------------------------------------------

	/**
	 * aFileName に指定する同じパッケージ内の Properties を読み込みます。
	 * 指定ファイルが存在しない場合はエラー。
	 */
	public static Properties loadNecessaryPackagePrivateProperties(Class<?> aClass, String aFileName) {
		try {
			return loadProperties(aClass, aFileName, aClass.getResourceAsStream(aFileName));
		} catch (NullPointerException e) {
			throw new RuntimeException(new FileNotFoundException(aClass.getPackage() + "." + aFileName));
		}
	}

	/**
	 * aFileName に指定する同じパッケージ内の Properties を読み込みます。
	 * 指定ファイルが存在しない場合は警告のみ。
	 */
	public static Properties loadUnnecessaryPackagePrivateProperties(Class<?> aClass, String aFileName) {
		try {
			return loadProperties(aClass, aFileName, aClass.getResourceAsStream(aFileName));
		} catch (NullPointerException e) {
			LogFactory.getLog(aClass).warn("not found : " + aClass.getPackage() + "." + aFileName);
			return new Properties();
		}
	}

	private static Properties loadProperties(Class<?> aClass, String aFileName, InputStream aResourceAsStream) throws NullPointerException {
		try {
			try {
				Properties tProperties = new Properties();
				tProperties.load(aResourceAsStream);
				return tProperties;
			} finally {
				aResourceAsStream.close();
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	// -----------------------------------------------------------------------------------------------------------------

}
