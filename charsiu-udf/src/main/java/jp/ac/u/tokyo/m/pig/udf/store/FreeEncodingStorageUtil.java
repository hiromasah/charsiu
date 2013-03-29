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

package jp.ac.u.tokyo.m.pig.udf.store;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.Map;

import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;

public class FreeEncodingStorageUtil {

	/**
	 * Overwrite {@link org.apache.pig.impl.util.StorageUtil.putField}, for free encoding.
	 * 
	 * @param aOut
	 *            an OutputStream object
	 * @param aField
	 *            an object to be serialized
	 * @param aEncoding
	 * @throws IOException
	 *             if serialization fails.
	 */
	@SuppressWarnings("unchecked")
	public static void putField(OutputStream aOut, Object aField, String aEncoding)
			throws IOException {
		// string constants for each delimiter
		String tupleBeginDelim = "(";
		String tupleEndDelim = ")";
		String bagBeginDelim = "{";
		String bagEndDelim = "}";
		String mapBeginDelim = "[";
		String mapEndDelim = "]";
		String fieldDelim = ",";
		String mapKeyValueDelim = "#";

		switch (DataType.findType(aField)) {
		case DataType.NULL:
			break; // just leave it empty

		case DataType.BOOLEAN:
			aOut.write(((Boolean) aField).toString().getBytes(aEncoding));
			break;

		case DataType.INTEGER:
			aOut.write(((Integer) aField).toString().getBytes(aEncoding));
			break;

		case DataType.LONG:
			aOut.write(((Long) aField).toString().getBytes(aEncoding));
			break;

		case DataType.FLOAT:
			aOut.write(((Float) aField).toString().getBytes(aEncoding));
			break;

		case DataType.DOUBLE:
			aOut.write(((Double) aField).toString().getBytes(aEncoding));
			break;

		case DataType.BYTEARRAY:
			byte[] b = ((DataByteArray) aField).get();
			aOut.write(b, 0, b.length);
			break;

		case DataType.CHARARRAY:
			// oddly enough, writeBytes writes a string
			aOut.write(((String) aField).getBytes(aEncoding));
			break;

		case DataType.MAP:
			boolean mapHasNext = false;
			Map<String, Object> m = (Map<String, Object>) aField;
			aOut.write(mapBeginDelim.getBytes(aEncoding));
			for (Map.Entry<String, Object> e : m.entrySet()) {
				if (mapHasNext) {
					aOut.write(fieldDelim.getBytes(aEncoding));
				} else {
					mapHasNext = true;
				}
				putField(aOut, e.getKey(), aEncoding);
				aOut.write(mapKeyValueDelim.getBytes(aEncoding));
				putField(aOut, e.getValue(), aEncoding);
			}
			aOut.write(mapEndDelim.getBytes(aEncoding));
			break;

		case DataType.TUPLE:
			boolean tupleHasNext = false;
			Tuple t = (Tuple) aField;
			aOut.write(tupleBeginDelim.getBytes(aEncoding));
			for (int i = 0; i < t.size(); ++i) {
				if (tupleHasNext) {
					aOut.write(fieldDelim.getBytes(aEncoding));
				} else {
					tupleHasNext = true;
				}
				try {
					putField(aOut, t.get(i), aEncoding);
				} catch (ExecException ee) {
					throw ee;
				}
			}
			aOut.write(tupleEndDelim.getBytes(aEncoding));
			break;

		case DataType.BAG:
			boolean bagHasNext = false;
			aOut.write(bagBeginDelim.getBytes(aEncoding));
			Iterator<Tuple> tupleIter = ((DataBag) aField).iterator();
			while (tupleIter.hasNext()) {
				if (bagHasNext) {
					aOut.write(fieldDelim.getBytes(aEncoding));
				} else {
					bagHasNext = true;
				}
				putField(aOut, (Object) tupleIter.next(), aEncoding);
			}
			aOut.write(bagEndDelim.getBytes(aEncoding));
			break;

		default: {
			int errCode = 2108;
			String msg = "Could not determine data type of field: " + aField;
			throw new ExecException(msg, errCode, PigException.BUG);
		}

		}
	}

}
