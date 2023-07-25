package com.ccp.implementations.db.crud.elasticsearch;

import com.ccp.decorators.CcpMapDecorator;
import com.ccp.process.CcpProcess;
class CcpSourceHandler implements CcpProcess{

	@Override
	public CcpMapDecorator execute(CcpMapDecorator x) {
		
		CcpMapDecorator error = x.getInternalMap("error");
		
		boolean hasError = error.isEmpty() == false;
		
		if(hasError) {
			String errorType = error.getAsString("type");
			String reason = error.getAsString("reason");
			throw new RuntimeException(errorType + ". Reason: " + reason);
		}

		CcpMapDecorator internalMap = x.getInternalMap("_source");
		Boolean found = x.getAsBoolean("found");
		String id = x.getAsString("_id");
		String _index = x.getAsString("_index");
		CcpMapDecorator put = internalMap.put("id", id).put("_found", found).put("_index", _index);
		
		return put;
	}
	
}