
package com.ccp.implementations.db.crud.elasticsearch;


import java.util.function.Function;

import com.ccp.decorators.CcpJsonRepresentation;

class SourceHandler implements  Function<CcpJsonRepresentation, CcpJsonRepresentation>{
	
	public SourceHandler() {
	}

	public CcpJsonRepresentation apply(CcpJsonRepresentation json) {
		
		CcpJsonRepresentation error = json.getInnerJson("error");
		
		boolean hasError = error.isEmpty() == false;
		
		if(hasError) {
			String errorType = error.getAsString("type");
			String reason = error.getAsString("reason");
			throw new RuntimeException(errorType + ". Reason: " + reason);
		}

		CcpJsonRepresentation internalMap = json.getInnerJson("_source");
		
		String _index = json.getAsString("_index");
		String id = json.getAsString("_id");

		CcpJsonRepresentation put = internalMap
				.put("_id", id)
				.put("_index", _index)
				;
		
		return put;
	}
	
}