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

public class DPCColumnSchema {

	private final String mName;
	private final String mType;

	public DPCColumnSchema(String aName, String aType) {
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
	public boolean equals(Object aTarget) {
		try {
			DPCColumnSchema tTarget = (DPCColumnSchema) aTarget;
			return mName.equals(tTarget.getName());
		} catch (ClassCastException e) {
			return false;
		}
	}
}
