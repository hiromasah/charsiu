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

package jp.ac.u.tokyo.m.dpc.pig.udf.load.path;

import java.util.Collection;

import junit.framework.Assert;

import org.junit.Test;

public class LoadFilesFormatParseUtilTest {

	// -----------------------------------------------------------------------------------------------------------------

	@Test
	public void testParseLoadFilesFormatIndexes_Single() throws Throwable {
		Collection<String> tInputFiles = LoadFilesFormatParseUtil.parseLoadFilesFormatIndexes("2008", "/dpc/indexes", "ff1");
		Object[] tInputFilesArray = tInputFiles.toArray();
		Assert.assertEquals(1, tInputFiles.size());
		Assert.assertEquals("/dpc/indexes/2008/ff1", tInputFilesArray[0]);
	}

	@Test
	public void testParseLoadFilesFormatIndexes_YearRange() throws Throwable {
		Collection<String> tInputFiles = LoadFilesFormatParseUtil.parseLoadFilesFormatIndexes("2008-2010", "/dpc/indexes", "ff1");
		Object[] tInputFilesArray = tInputFiles.toArray();
		Assert.assertEquals(3, tInputFiles.size());
		Assert.assertEquals("/dpc/indexes/2008/ff1", tInputFilesArray[0]);
		Assert.assertEquals("/dpc/indexes/2009/ff1", tInputFilesArray[1]);
		Assert.assertEquals("/dpc/indexes/2010/ff1", tInputFilesArray[2]);
	}
	
	@Test
	public void testParseLoadFilesFormatIndexes_Combi() throws Throwable {
		Collection<String> tInputFiles = LoadFilesFormatParseUtil.parseLoadFilesFormatIndexes("2000,2008-2010", "/dpc/indexes", "ff1");
		Object[] tInputFilesArray = tInputFiles.toArray();
		Assert.assertEquals(4, tInputFiles.size());
		Assert.assertEquals("/dpc/indexes/2000/ff1", tInputFilesArray[0]);
		Assert.assertEquals("/dpc/indexes/2008/ff1", tInputFilesArray[1]);
		Assert.assertEquals("/dpc/indexes/2009/ff1", tInputFilesArray[2]);
		Assert.assertEquals("/dpc/indexes/2010/ff1", tInputFilesArray[3]);
	}
	
	// -----------------------------------------------------------------------------------------------------------------

}
