
package com.ccp.implementations.db.dao.elasticsearch;


import java.util.function.Function;

import com.ccp.decorators.CcpJsonRepresentation;

class SourceHandler implements  Function<CcpJsonRepresentation, CcpJsonRepresentation>{
	private final CcpJsonRepresentation originalQuery;
	
	public SourceHandler(CcpJsonRepresentation originalQuery) {
		this.originalQuery = originalQuery;
	}

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
		String _index = x.getAsString("_index");
		String id = x.getAsString("_id");

		CcpJsonRepresentation put = internalMap.put("id", id)
				.put("_originalQuery", this.originalQuery)
				.put("_index", _index)
				.put("_found", found)
				;
		
		return put;
	}
	
}