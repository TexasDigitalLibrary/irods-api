/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.umiacs.irods.api.pi;

import java.util.Date;
import java.util.List;

/**
 * #define RodsObjStat_PI "double objSize; int objType; int numCopies; str
 * dataId[NAME_LEN]; str chksum[NAME_LEN]; str ownerName[NAME_LEN]; str
 * ownerZone[NAME_LEN]; str createTime[TIME_LEN]; str modifyTime[TIME_LEN];
 * struct *SpecColl_PI;"
 * 
 * @author toaster
 */
public class RodsObjStat_PI implements IRodsPI {
	private long objSize;
	private ObjTypeEnum objType;
	private int numCopies;
	private String dataId;
	private String chksum;
	private String ownerName;
	private String ownerZone;
	private Date createTime;
	private Date modifyTime;

	private byte[] bytes;

	// TODO SpecColl
	public RodsObjStat_PI(ProtocolToken pt) throws ParseException {
		pt.checkName("RodsObjStat_PI");
		List<ProtocolToken> tokens = ProtocolToken.parseTokens(pt.getValue());
		// LOG.debug("tok: " + pt.getValue());
		tokens.get(0).checkName("objSize");
		objSize = tokens.get(0).getLongValue();

		tokens.get(1).checkName("objType");
		objType = ObjTypeEnum.valueOf(tokens.get(1).getIntValue());

		if (tokens.get(2).hasName("numCopies")
				|| tokens.get(2).hasName("dataMode")) {
			numCopies = tokens.get(2).getIntValue();
		} else {
			throw new ParseException("numCopies or dataMode expected");
		}

		tokens.get(3).checkName("dataId");
		dataId = tokens.get(3).getValue();

		tokens.get(4).checkName("chksum");
		chksum = tokens.get(4).getValue();

		tokens.get(5).checkName("ownerName");
		ownerName = tokens.get(5).getValue();

		tokens.get(6).checkName("ownerZone");
		ownerZone = tokens.get(6).getValue();

		tokens.get(7).checkName("createTime");
		createTime = new Date(tokens.get(7).getLongValue() * 1000);

		tokens.get(8).checkName("modifyTime");
		modifyTime = new Date(tokens.get(8).getLongValue() * 1000);

	}

	@Override
	public String toString() {
		return "<RodsObjStat_PI>" + "<objSize>" + objSize + "</objSize>"
				+ "<objType>" + objType + "<objType>" + "<dataMode>"
				+ numCopies + "</dataMode>" + "<dataId>" + dataId + "</dataId>"
				+ "<chksum>" + chksum + "</chksum>" + "<ownerName>" + ownerName
				+ "</ownerName>" + "<ownerZone>" + ownerZone + "</ownerZone>"
				+ "<createTime>" + createTime + "</createTime>"
				+ "<modifyTime>" + modifyTime + "</modifyTime>"
				+ "</RodsObjStat_PI>";
	}

	public byte[] getBytes() {
		if (bytes == null) {
			bytes = toString().getBytes();
		}
		return bytes;
	}

	public long getObjSize() {
		return objSize;
	}

	public ObjTypeEnum getObjType() {
		return objType;
	}

	/**
	 * originally numCopies, in protocol d, this was changed to dataMode left
	 * for compatibility.
	 * 
	 * @return numCopies or dataMode
	 */
	public int getNumCopies() {
		return numCopies;
	}

	/**
	 * return datamode
	 * 
	 * @return datamode, same as numcopies, in protocol d, this was renamed
	 */
	public int getDataMode() {
		return numCopies;
	}

	public String getDataId() {
		return dataId;
	}

	public String getChksum() {
		return chksum;
	}

	public String getOwnerName() {
		return ownerName;
	}

	public String getOwnerZone() {
		return ownerZone;
	}

	public Date getCreateTime() {
		return createTime;
	}

	public Date getModifyTime() {
		return modifyTime;
	}

}
