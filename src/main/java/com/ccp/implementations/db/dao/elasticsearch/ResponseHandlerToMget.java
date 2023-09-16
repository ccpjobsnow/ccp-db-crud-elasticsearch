package com.ccp.implementations.db.dao.elasticsearch;

import com.ccp.decorators.CcpMapDecorator;

class SourceHandler implements  java.util.function.Function<CcpMapDecorator, CcpMapDecorator>{

	@Override
	public CcpMapDecorator apply(CcpMapDecorator x) {
		
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