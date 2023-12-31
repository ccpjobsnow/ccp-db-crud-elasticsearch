package com.ccp.implementations.db.dao.elasticsearch;

import com.ccp.decorators.CcpJsonRepresentation;

class SourceHandler implements  java.util.function.Function<CcpJsonRepresentation, CcpJsonRepresentation>{

	@Override
	public CcpJsonRepresentation apply(CcpJsonRepresentation x) {
		
		CcpJsonRepresentation error = x.getInnerJson("error");
		
		boolean hasError = error.isEmpty() == false;
		
		if(hasError) {
			String errorType = error.getAsString("type");
			String reason = error.getAsString("reason");
			throw new RuntimeException(errorType + ". Reason: " + reason);
		}

		CcpJsonRepresentation internalMap = x.getInnerJson("_source");
		Boolean found = x.getAsBoolean("found");
		String id = x.getAsString("_id");
		String _index = x.getAsString("_index");
		CcpJsonRepresentation put = internalMap.put("id", id).put("_found", found).put("_index", _index);
		
		return put;
	}
	
}