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
	 * This method read Ini in the same package. <br>
	 * If the file does not exist, throws exception. <br>
	 * <br>
	 * aFileName に指定する同じパッケージ内の Ini を読み込みます。 <br>
	 * 指定ファイルが存在しない場合はエラー。 <br>
	 */
	public static Ini loadNecessaryPackagePrivateIni(Class<?> aClass, String aFileName) {
		return loadNecessaryIni(aClass, aFileName, aClass.getResourceAsStream(aFileName));
	}

	/**
	 * This method read Ini in the jar. <br>
	 * If the file does not exist, throws exception. <br>
	 * <br>
	 * aFileName に指定するjar内の Ini を読み込みます。 <br>
	 * 指定ファイルが存在しない場合はエラー。 <br>
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
	 * This method read Ini in the same package. <br>
	 * If the file does not exist, logging WARN. <br>
	 * <br>
	 * aFileName に指定する同じパッケージ内の Ini を読み込みます。 <br>
	 * 指定ファイルが存在しない場合は警告のみ。 <br>
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
	 * This method read Properties in the same package. <br>
	 * If the file does not exist, throws exception. <br>
	 * <br>
	 * aFileName に指定する同じパッケージ内の Properties を読み込みます。 <br>
	 * 指定ファイルが存在しない場合はエラー。 <br>
	 */
	public static Properties loadNecessaryPackagePrivateProperties(Class<?> aClass, String aFileName) {
		try {
			return loadProperties(aClass, aFileName, aClass.getResourceAsStream(aFileName));
		} catch (NullPointerException e) {
			throw new RuntimeException(new FileNotFoundException(aClass.getPackage() + "." + aFileName));
		}
	}

	/**
	 * This method read Properties in the same package. <br>
	 * If the file does not exist, logging WARN. <br>
	 * <br>
	 * aFileName に指定する同じパッケージ内の Properties を読み込みます。 <br>
	 * 指定ファイルが存在しない場合は警告のみ。 <br>
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
