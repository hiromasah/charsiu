package jp.ac.u.tokyo.m.dpc.pig.udf.load.path;

import org.apache.hadoop.fs.FileStatus;

public class FileStatusWithVersion {
	private FileStatus mFileStatus;
	private String mVersion;

	public FileStatusWithVersion(FileStatus aFileStatus, String aVersion) {
		setFileStatus(aFileStatus);
		setVersion(aVersion);
	}

	public void setFileStatus(FileStatus fileStatus) {
		mFileStatus = fileStatus;
	}

	public FileStatus getFileStatus() {
		return mFileStatus;
	}

	public void setVersion(String version) {
		mVersion = version;
	}

	public String getVersion() {
		return mVersion;
	}
}
