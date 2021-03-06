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

package jp.ac.u.tokyo.m.pig.udf.eval.util;

import java.util.regex.Pattern;

public class ReflectionUDFSetting {

	// -----------------------------------------------------------------------------------------------------------------

	private final String mClassName;
	private final String mColumnRegex;
	private final String mUDFArgumentFormat;
	private final String mAliasSuffix;

	private final Pattern mColumnPattern;

	// -----------------------------------------------------------------------------------------------------------------

	public ReflectionUDFSetting(String aClassName, String aColumnRegex, String aUDFArgumentFormat, String aAliasSuffix) {
		mClassName = aClassName;
		mColumnRegex = aColumnRegex;
		mUDFArgumentFormat = aUDFArgumentFormat;
		mAliasSuffix = aAliasSuffix;

		mColumnPattern = Pattern.compile(aColumnRegex);
	}

	@Override
	public String toString() {
		return "ReflectionUDFSetting [mClassName=" + mClassName + ", mColumnRegex=" + mColumnRegex + ", mUDFArgumentFormat=" + mUDFArgumentFormat + ", mAliasSuffix=" + mAliasSuffix + ", mColumnPattern=" + mColumnPattern + "]";
	}

	// -----------------------------------------------------------------------------------------------------------------

	public String getClassName() {
		return mClassName;
	}

	public String getColumnRegex() {
		return mColumnRegex;
	}

	public String getUDFArgumentFormat() {
		return mUDFArgumentFormat;
	}

	public String getAliasSuffix() {
		return mAliasSuffix;
	}

	public Pattern getColumnPattern() {
		return mColumnPattern;
	}

	// -----------------------------------------------------------------------------------------------------------------

	public boolean matchesColumnRegex(String aTarget) {
		return getColumnPattern().matcher(aTarget).matches();
	}

	// -----------------------------------------------------------------------------------------------------------------

}
