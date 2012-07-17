package jp.ac.u.tokyo.m.dpc.pig.udf.load.mapping;

public interface ResultColumn {
	Object getValue(String[] aValues);
}
