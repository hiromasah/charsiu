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

public class ColumnSchema {

	private String mName;
	private transient String mType;

	public ColumnSchema(String aName, String aType) {
		mName = aName;
		mType = aType;
	}

	public String getName() {
		return mName;
	}

	public String getType() {
		return mType;
	}

	@Override
	public String toString() {
		return mName + " : " + mType;
	}

	@Override
	public boolean equals(Object aTarget) {
		try {
			ColumnSchema tTarget = (ColumnSchema) aTarget;
			return mName.equals(tTarget.getName());
		} catch (ClassCastException e) {
			return false;
		}
	}

}
