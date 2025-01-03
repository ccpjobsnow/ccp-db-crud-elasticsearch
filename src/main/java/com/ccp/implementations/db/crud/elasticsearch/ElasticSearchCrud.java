package com.ccp.implementations.db.crud.elasticsearch;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.ccp.constantes.CcpOtherConstants;
import com.ccp.decorators.CcpJsonRepresentation;
import com.ccp.decorators.CcpTimeDecorator;
import com.ccp.dependency.injection.CcpDependencyInjection;
import com.ccp.especifications.db.crud.CcpCrud;
import com.ccp.especifications.db.crud.CcpSelectUnionAll;
import com.ccp.especifications.db.crud.CcpUnionAllExecutor;
import com.ccp.especifications.db.utils.CcpDbRequester;
import com.ccp.especifications.db.utils.CcpEntity;
import com.ccp.especifications.http.CcpHttpResponseType;
import com.ccp.exceptions.db.CcpEntityRecordNotFound;
import com.ccp.exceptions.process.CcpThrowException;

class ElasticSearchCrud implements CcpCrud, CcpUnionAllExecutor {

	private CcpJsonRepresentation getRequestBodyToMultipleGet(Collection<CcpJsonRepresentation> jsons, CcpEntity... entities) {
		
		List<CcpJsonRepresentation> docs = new ArrayList<CcpJsonRepresentation>();
		
		for (CcpEntity entity : entities) {
			
			for (CcpJsonRepresentation json : jsons) {
				
 				List<String> primaryKeyNames = entity.getPrimaryKeyNames();
				
				boolean anyKeyIsMissing = json.containsAllFields(primaryKeyNames) == false;
				
				if(anyKeyIsMissing) {
					continue;
				}

				List<CcpJsonRepresentation> parametersToSearch = entity.getParametersToSearch(json);
				docs.addAll(parametersToSearch);
			}
		}
		
		boolean idsNotFound = docs.isEmpty();
	
		if(idsNotFound) {
			throw new RuntimeException(String.format(" Ids not found in the values %s  to the entities '%s' ", jsons, Arrays.asList(entities)));
		}
		
		CcpJsonRepresentation requestBody = CcpOtherConstants.EMPTY_JSON.put("docs", docs);
		return requestBody;
	}
	
	public CcpJsonRepresentation getRequestBodyToMultipleGet(Set<String> ids, CcpEntity... entities) {
		List<CcpJsonRepresentation> docs1 = new ArrayList<CcpJsonRepresentation>();
		for (CcpEntity entity : entities) {
			String entidade = entity.getEntityName();
			for (String id : ids) {
				CcpJsonRepresentation put = CcpOtherConstants.EMPTY_JSON
				.put("_index", entidade)
				.put("_id", id)
				;
				docs1.add(put);
			}
		}
		CcpJsonRepresentation requestBody = CcpOtherConstants.EMPTY_JSON.put("docs", docs1);
		return requestBody;
	}
	
	public CcpJsonRepresentation getOneById(String entityName, String id) {
		String path = "/" + entityName + "/_source/" + id ;
		
		CcpJsonRepresentation handlers = CcpOtherConstants.EMPTY_JSON.addJsonTransformer("200", CcpOtherConstants.DO_NOTHING).addJsonTransformer("404", new CcpThrowException(new CcpEntityRecordNotFound(entityName, id)));
		
		CcpDbRequester dbUtils = CcpDependencyInjection.getDependency(CcpDbRequester.class);
		CcpJsonRepresentation response = dbUtils.executeHttpRequest("getOneById", path, "GET", handlers, CcpOtherConstants.EMPTY_JSON, CcpHttpResponseType.singleRecord);
		
		return response;
	}

	public boolean exists(String entityName, String id) {
		String path = "/" + entityName + "/_doc/" + id;
		
		CcpJsonRepresentation flows = CcpOtherConstants.EMPTY_JSON.addJsonTransformer("200", ElasticSearchHttpStatus.OK).addJsonTransformer("404",  ElasticSearchHttpStatus.NOT_FOUND);
		
		CcpDbRequester dbUtils = CcpDependencyInjection.getDependency(CcpDbRequester.class);
		CcpJsonRepresentation response = dbUtils.executeHttpRequest("exists", path, "HEAD", flows, CcpOtherConstants.EMPTY_JSON, CcpHttpResponseType.singleRecord);
		ElasticSearchHttpStatus status = response.getAsObject(ElasticSearchHttpStatus.class.getSimpleName());
		
		boolean exists = ElasticSearchHttpStatus.OK.equals(status);
		return exists;
	}

	public CcpJsonRepresentation createOrUpdate(String entityName, CcpJsonRepresentation json, String id) {
		String path = "/" + entityName + "/_update/" + id;
		
		CcpJsonRepresentation requestBody = CcpOtherConstants.EMPTY_JSON
				.addToItem("script", "lang", "painless")
				.addToItem("script", "source", "ctx._source.putAll(params);")
				.addToItem("script", "params", json)
				.put("upsert", json)
				;
		
		CcpJsonRepresentation handlers = CcpOtherConstants.EMPTY_JSON
				.addJsonTransformer("409", values -> this.retryCreateOrUpdate(entityName, json, id))
				.addJsonTransformer("201",  ElasticSearchHttpStatus.CREATED)
				.addJsonTransformer("200", ElasticSearchHttpStatus.OK)
				;
		
		CcpDbRequester dbUtils = CcpDependencyInjection.getDependency(CcpDbRequester.class);
		CcpJsonRepresentation response = dbUtils.executeHttpRequest("createOrUpdate", path, "POST", handlers, requestBody, CcpHttpResponseType.singleRecord);
		return response;
	}

	private CcpJsonRepresentation retryCreateOrUpdate(String entityName, CcpJsonRepresentation json, String id) {
		new CcpTimeDecorator().sleep(1000);
		CcpJsonRepresentation createOrUpdate = this.createOrUpdate(entityName, json, id);
		return createOrUpdate;
	}

	public boolean delete(String entityName, String id) {
		String path = "/" + entityName + "/_doc/" + id;
		CcpJsonRepresentation handlers = CcpOtherConstants.EMPTY_JSON.addJsonTransformer("200", CcpOtherConstants.DO_NOTHING).addJsonTransformer("404", CcpOtherConstants.DO_NOTHING);
		CcpDbRequester dbUtils = CcpDependencyInjection.getDependency(CcpDbRequester.class);
		CcpJsonRepresentation response = dbUtils.executeHttpRequest("delete", path, "DELETE", handlers, CcpOtherConstants.EMPTY_JSON, CcpHttpResponseType.singleRecord);
		String result = response.getAsString("result");
		boolean found = "deleted".equals( result);
		return found;
	}
	
	public CcpSelectUnionAll unionAll(Collection<CcpJsonRepresentation> values, CcpEntity... entities) {
		CcpJsonRepresentation requestBody = this.getRequestBodyToMultipleGet(values, entities);
		CcpSelectUnionAll ccpSelectUnionAll = this.unionAll(requestBody);
		return ccpSelectUnionAll;
	}

	private CcpSelectUnionAll unionAll(CcpJsonRepresentation requestBody) {
		SourceHandler mgetHandler = new SourceHandler();
		CcpDbRequester dbUtils = CcpDependencyInjection.getDependency(CcpDbRequester.class);
		CcpJsonRepresentation response = dbUtils.executeHttpRequest("getResponseToMultipleGet", "/_mget", "POST", 200, requestBody, CcpHttpResponseType.singleRecord);
		List<CcpJsonRepresentation> docs = response.getAsJsonList("docs");
		List<CcpJsonRepresentation> asMapList = docs.stream().map(mgetHandler).collect(Collectors.toList());
		CcpSelectUnionAll ccpSelectUnionAll = new CcpSelectUnionAll(asMapList);
		return ccpSelectUnionAll;
	}

	public CcpUnionAllExecutor getUnionAllExecutor() {
		return this;
	}
}
