package com.ccp.implementations.db.dao.elasticsearch;

import com.ccp.dependency.injection.CcpInstanceProvider;
import com.ccp.especifications.db.dao.CcpDao;

public class CcpElasticSearchDao implements CcpInstanceProvider<CcpDao>  {

	
	public CcpDao getInstance() {
		return new ElasticSearchDao();
	}

}
