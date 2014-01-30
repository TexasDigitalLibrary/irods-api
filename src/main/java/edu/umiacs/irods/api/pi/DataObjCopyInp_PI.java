package edu.umiacs.irods.api.pi;

import java.util.Map;

/**
 * #define DataObjCopyInp_PI "struct DataObjInp_PI; struct DataObjInp_PI;"
 */
public class DataObjCopyInp_PI implements IRodsPI {
	private DataObjInp_PI source;
	private DataObjInp_PI dest;

	private byte[] bytes;

	public DataObjCopyInp_PI(String source, String dest) {
		this.source = new DataObjInp_PI(source, 0, 0, 0, 0, 0,
				OprTypeEnum.COPY_SRC, new KeyValPair_PI(
						(Map<String, String>) null));
		this.dest = new DataObjInp_PI(dest, 0, 0, 0, 0, 0,
				OprTypeEnum.COPY_DEST, new KeyValPair_PI(
						(Map<String, String>) null));
	}

	@Override
	public String toString() {
		String ret = "<DataObjCopyInp_PI>";
		ret += source.toString();
		ret += dest.toString();
		ret += "</DataObjCopyInp_PI>";
		return ret;
	}

	public byte[] getBytes() {
		if (bytes == null)
			bytes = toString().getBytes();
		return bytes;
	}

}
