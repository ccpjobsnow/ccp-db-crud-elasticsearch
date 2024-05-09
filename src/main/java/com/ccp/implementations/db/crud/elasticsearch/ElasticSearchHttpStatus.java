package com.ccp.implementations.db.crud.elasticsearch;

import java.util.function.Function;

import com.ccp.decorators.CcpJsonRepresentation;

public enum ElasticSearchHttpStatus implements  Function<CcpJsonRepresentation, CcpJsonRepresentation>{
	OK,
	NOT_FOUND, 
	CREATED;


	
	public CcpJsonRepresentation apply(CcpJsonRepresentation values) {
		CcpJsonRepresentation put = values.put(ElasticSearchHttpStatus.class.getSimpleName(), this);
		return put;
	}
	
}

