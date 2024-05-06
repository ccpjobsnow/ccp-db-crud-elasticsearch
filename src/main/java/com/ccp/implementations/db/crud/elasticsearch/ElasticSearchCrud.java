package com.ccp.implementations.db.crud.elasticsearch;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.ccp.constantes.CcpConstants;
import com.ccp.decorators.CcpJsonRepresentation;
import com.ccp.decorators.CcpTimeDecorator;
import com.ccp.dependency.injection.CcpDependencyInjection;
import com.ccp.especifications.db.crud.CcpCrud;
import com.ccp.especifications.db.crud.CcpSelectUnionAll;
import com.ccp.especifications.db.utils.CcpDbRequester;
import com.ccp.especifications.db.utils.CcpEntity;
import com.ccp.especifications.http.CcpHttpResponseType;
import com.ccp.exceptions.db.CcpEntityRecordNotFound;
import com.ccp.exceptions.process.CcpThrowException;

class ElasticSearchCrud implements CcpCrud {


	public CcpJsonRepresentation getRequestBodyToMultipleGet(Collection<CcpJsonRepresentation> values, CcpEntity... entities) {
		List<CcpJsonRepresentation> docs1 = new ArrayList<CcpJsonRepresentation>();
		for (CcpEntity entity : entities) {
			String entidade = entity.getEntityName();
			for (CcpJsonRepresentation value : values) {
				String id = entity.getId(value);
				CcpJsonRepresentation put = CcpConstants.EMPTY_JSON
				.put("_index", entidade)
				.put("_id", id)
				;
				docs1.add(put);
			}
		}
		CcpJsonRepresentation requestBody = CcpConstants.EMPTY_JSON.put("docs", docs1);
		return requestBody;
	}

	public CcpJsonRepresentation getRequestBodyToMultipleGet(Set<String> ids, CcpEntity... entities) {
		List<CcpJsonRepresentation> docs1 = new ArrayList<CcpJsonRepresentation>();
		for (CcpEntity entity : entities) {
			String entidade = entity.getEntityName();
			for (String id : ids) {
				CcpJsonRepresentation put = CcpConstants.EMPTY_JSON
				.put("_id", id)
				.put("_index", entidade);
				docs1.add(put);
			}
		}
		CcpJsonRepresentation requestBody = CcpConstants.EMPTY_JSON.put("docs", docs1);
		return requestBody;
	}
	
	public CcpJsonRepresentation getOneById(CcpEntity entity, CcpJsonRepresentation values) {
	
		String id = entity.getId(values);
		
		CcpJsonRepresentation oneById = this.getOneById(entity, id);
		
		return oneById;
	}

	
	public CcpJsonRepresentation getOneById(CcpEntity entity, String id) {
		String path = "/" + entity + "/_source/" + id ;
		
		String entityName = entity.getEntityName();
		CcpJsonRepresentation handlers = CcpConstants.EMPTY_JSON.put("200", CcpConstants.DO_BY_PASS).put("404", new CcpThrowException(new CcpEntityRecordNotFound(entityName, id)));
		
		CcpDbRequester dbUtils = CcpDependencyInjection.getDependency(CcpDbRequester.class);
		CcpJsonRepresentation response = dbUtils.executeHttpRequest("getOneById", path, "GET", handlers, CcpConstants.EMPTY_JSON, CcpHttpResponseType.singleRecord);
		
		return response;
	}

	public boolean exists(CcpEntity entity, String id) {
		String path = "/" + entity + "/_doc/" + id;
		
		CcpJsonRepresentation flows = CcpConstants.EMPTY_JSON.put("200", CcpHttpStatus.OK).put("404",  CcpHttpStatus.NOT_FOUND);
		
		CcpDbRequester dbUtils = CcpDependencyInjection.getDependency(CcpDbRequester.class);
		CcpJsonRepresentation response = dbUtils.executeHttpRequest("exists", path, "HEAD", flows, CcpConstants.EMPTY_JSON, CcpHttpResponseType.singleRecord);
		CcpHttpStatus status = response.getAsObject(CcpHttpStatus.class.getSimpleName());
		
		boolean exists = CcpHttpStatus.OK.equals(status);
		return exists;
	}

	
	public CcpJsonRepresentation createOrUpdate(CcpEntity entity, CcpJsonRepresentation data, String id) {
		CcpJsonRepresentation onlyExistingFields = entity.getOnlyExistingFields(data);
		String path = "/" + entity + "/_update/" + id;
		
		CcpJsonRepresentation requestBody = CcpConstants.EMPTY_JSON
				.putSubKey("script", "lang", "painless")
				.putSubKey("script", "source", "ctx._source.putAll(params);")
				.putSubKey("script", "params", onlyExistingFields)
				.put("upsert", onlyExistingFields)
				;
		
		CcpJsonRepresentation handlers = CcpConstants.EMPTY_JSON
				.put("409", values -> this.retryCreateOrUpdate(entity, data, id))
				.put("201",  CcpHttpStatus.CREATED)
				.put("200", CcpHttpStatus.OK)
				;
		
		CcpDbRequester dbUtils = CcpDependencyInjection.getDependency(CcpDbRequester.class);
		CcpJsonRepresentation response = dbUtils.executeHttpRequest("createOrUpdate", path, "POST", handlers, requestBody, CcpHttpResponseType.singleRecord);
		return response;
	}

	private CcpJsonRepresentation retryCreateOrUpdate(CcpEntity entity, CcpJsonRepresentation data, String id) {
		new CcpTimeDecorator().sleep(1000);
		return this.createOrUpdate(entity, data, id);
	}

	
	public boolean delete(CcpEntity entity, String id) {
		String path = "/" + entity + "/_doc/" + id;
		CcpJsonRepresentation handlers = CcpConstants.EMPTY_JSON.put("200", CcpConstants.DO_BY_PASS).put("404", CcpConstants.DO_BY_PASS);
		CcpDbRequester dbUtils = CcpDependencyInjection.getDependency(CcpDbRequester.class);
		CcpJsonRepresentation response = dbUtils.executeHttpRequest("delete", path, "DELETE", handlers, CcpConstants.EMPTY_JSON, CcpHttpResponseType.singleRecord);
		String result = response.getAsString("result");
		boolean found = "deleted".equals( result);
		return found;
	}

	public CcpSelectUnionAll unionAll(Collection<CcpJsonRepresentation> values, CcpEntity... entities) {
		SourceHandler mgetHandler = new SourceHandler();
		CcpJsonRepresentation requestBody = this.getRequestBodyToMultipleGet(values, entities);
		CcpDbRequester dbUtils = CcpDependencyInjection.getDependency(CcpDbRequester.class);
		CcpJsonRepresentation response = dbUtils.executeHttpRequest("getResponseToMultipleGet", "/_mget", "POST", 200, requestBody, CcpHttpResponseType.singleRecord);
		List<CcpJsonRepresentation> docs = response.getAsJsonList("docs");
		List<CcpJsonRepresentation> asMapList = docs.stream().map(mgetHandler).collect(Collectors.toList());
		CcpSelectUnionAll ccpSelectUnionAll = new CcpSelectUnionAll(asMapList);
		return ccpSelectUnionAll;
	}

}
enum CcpHttpStatus implements  java.util.function.Function<CcpJsonRepresentation, CcpJsonRepresentation>{
	OK,
	NOT_FOUND, 
	CREATED;


	
	public CcpJsonRepresentation apply(CcpJsonRepresentation values) {
		return values.put(CcpHttpStatus.class.getSimpleName(), this);
	}
	
}
