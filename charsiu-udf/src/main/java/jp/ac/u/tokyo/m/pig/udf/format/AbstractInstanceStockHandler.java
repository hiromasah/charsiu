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

package jp.ac.u.tokyo.m.pig.udf.format;

import java.util.LinkedHashMap;

import jp.ac.u.tokyo.m.pig.udf.BehaviorConstants;

public abstract class AbstractInstanceStockHandler<KEY, VALUE> {

	// -----------------------------------------------------------------------------------------------------------------

	private LinkedHashMap<KEY, VALUE> mInstanceStock = new LinkedHashMap<KEY, VALUE>();

	// -----------------------------------------------------------------------------------------------------------------

	protected void put(KEY aKey, VALUE aValue) {
		LinkedHashMap<KEY, VALUE> tInstanceStock = mInstanceStock;
		tInstanceStock.put(aKey, aValue);
		// Upper limit processing | 上限処理
		if (tInstanceStock.size() > BehaviorConstants.INSTANCE_STOCK_SIZE) {
			tInstanceStock.remove(tInstanceStock.keySet().iterator().next());
		}
	}

	protected VALUE get(KEY aKey) {
		return mInstanceStock.get(aKey);
	}

	protected LinkedHashMap<KEY, VALUE> getInstanceList() {
		return mInstanceStock;
	}

	// -----------------------------------------------------------------------------------------------------------------

}
