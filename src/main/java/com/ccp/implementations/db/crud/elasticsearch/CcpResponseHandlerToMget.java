package com.ccp.implementations.db.crud.elasticsearch;

import com.ccp.decorators.CcpMapDecorator;
import com.ccp.process.CcpProcess;
class CcpSourceHandler implements CcpProcess{

	@Override
	public CcpMapDecorator execute(CcpMapDecorator x) {
		CcpMapDecorator internalMap = x.getInternalMap("_source");
		String id = x.getAsString("_id");
		CcpMapDecorator put = internalMap.put("id", id);
		return put;
	}
	
}