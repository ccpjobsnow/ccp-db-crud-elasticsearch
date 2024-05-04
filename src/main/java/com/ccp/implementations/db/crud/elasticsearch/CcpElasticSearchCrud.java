package com.ccp.implementations.db.crud.elasticsearch;

import com.ccp.dependency.injection.CcpInstanceProvider;
import com.ccp.especifications.db.crud.CcpCrud;

public class CcpElasticSearchCrud implements CcpInstanceProvider<CcpCrud>  {

	
	public CcpCrud getInstance() {
		return new ElasticSearchCrud();
	}

}
