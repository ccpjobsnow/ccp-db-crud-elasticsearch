
package com.ccp.implementations.db.crud.elasticsearch;


import java.util.function.Function;

import com.ccp.decorators.CcpJsonRepresentation;
import com.ccp.exceptions.db.crud.CcpMultiGetSearchFailed;

class ResponseHandlerToMget implements Function<CcpJsonRepresentation, CcpJsonRepresentation>{
	
	static ResponseHandlerToMget INSTANCE = new ResponseHandlerToMget();
	
	private ResponseHandlerToMget() {}

	public CcpJsonRepresentation apply(CcpJsonRepresentation json) {
		
		CcpJsonRepresentation error = json.getInnerJson("error");
		
		boolean hasError = error.isEmpty() == false;
		
		if(hasError) {
			throw new CcpMultiGetSearchFailed(error);
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