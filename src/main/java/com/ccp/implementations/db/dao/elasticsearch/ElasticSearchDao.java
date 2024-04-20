package com.ccp.implementations.db.dao.elasticsearch;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.ccp.constantes.CcpConstants;
import com.ccp.decorators.CcpJsonRepresentation;
import com.ccp.decorators.CcpTimeDecorator;
import com.ccp.dependency.injection.CcpDependencyInjection;
import com.ccp.especifications.db.dao.CcpDao;
import com.ccp.especifications.db.dao.CcpDaoUnionAll;
import com.ccp.especifications.db.utils.CcpDbRequester;
import com.ccp.especifications.db.utils.CcpEntityIdGenerator;
import com.ccp.especifications.http.CcpHttpResponseType;
import com.ccp.exceptions.db.CcpEntityRecordNotFound;
import com.ccp.exceptions.process.CcpThrowException;

class ElasticSearchDao implements CcpDao {


	public List<CcpJsonRepresentation> getManyByIds(Function<CcpJsonRepresentation, CcpJsonRepresentation> mgetHandler, Collection<CcpJsonRepresentation> values, CcpEntityIdGenerator... entities) {
		CcpJsonRepresentation requestBody = this.getRequestBodyToMultipleGet(values, entities);
		List<CcpJsonRepresentation> collect = this.getResponseToMultipleGet(mgetHandler, requestBody);
		return collect;
	}

	public List<CcpJsonRepresentation> getManyByIds(Function<CcpJsonRepresentation, CcpJsonRepresentation> mgetHandler, Set<String> values, CcpEntityIdGenerator... entities) {
		CcpJsonRepresentation requestBody = this.getRequestBodyToMultipleGet(values, entities);
		List<CcpJsonRepresentation> collect = this.getResponseToMultipleGet(mgetHandler, requestBody);
		return collect;
	}

	public List<CcpJsonRepresentation> getResponseToMultipleGet(Function<CcpJsonRepresentation, CcpJsonRepresentation> mgetHandler, CcpJsonRepresentation requestBody) {
		CcpDbRequester dbUtils = CcpDependencyInjection.getDependency(CcpDbRequester.class);
		CcpJsonRepresentation response = dbUtils.executeHttpRequest("getResponseToMultipleGet", "/_mget", "POST", 200, requestBody, CcpHttpResponseType.singleRecord);
		List<CcpJsonRepresentation> docs = response.getAsJsonList("docs");
		List<CcpJsonRepresentation> collect = docs.stream().map(mgetHandler).collect(Collectors.toList());
		return collect;
	}

	public CcpJsonRepresentation getRequestBodyToMultipleGet(Collection<CcpJsonRepresentation> values, CcpEntityIdGenerator... entities) {
		List<CcpJsonRepresentation> docs1 = new ArrayList<CcpJsonRepresentation>();
		for (CcpEntityIdGenerator entity : entities) {
			String entidade = entity.name();
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

	public CcpJsonRepresentation getRequestBodyToMultipleGet(Set<String> ids, CcpEntityIdGenerator... entities) {
		List<CcpJsonRepresentation> docs1 = new ArrayList<CcpJsonRepresentation>();
		for (CcpEntityIdGenerator entity : entities) {
			String entidade = entity.name();
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
	
	public List<CcpJsonRepresentation> getManyById(CcpJsonRepresentation values, CcpEntityIdGenerator... entities) {

		SourceHandler mgetHandler = new SourceHandler(values);
		List<CcpJsonRepresentation> asList = Arrays.asList(values);
		List<CcpJsonRepresentation> asMapList = this.getManyByIds(mgetHandler, asList, entities);
		
		return asMapList;
	}

	
	public CcpJsonRepresentation getOneById(CcpEntityIdGenerator entity, CcpJsonRepresentation values) {
	
		String id = entity.getId(values);
		
		CcpJsonRepresentation oneById = this.getOneById(entity, id);
		
		return oneById;
	}

	
	public CcpJsonRepresentation getOneById(CcpEntityIdGenerator entity, String id) {
		String path = "/" + entity + "/_source/" + id ;
		
		CcpJsonRepresentation handlers = CcpConstants.EMPTY_JSON.put("200", CcpConstants.DO_NOTHING).put("404", new CcpThrowException(new CcpEntityRecordNotFound(entity.name(), id)));
		
		CcpDbRequester dbUtils = CcpDependencyInjection.getDependency(CcpDbRequester.class);
		CcpJsonRepresentation response = dbUtils.executeHttpRequest("getOneById", path, "GET", handlers, CcpConstants.EMPTY_JSON, CcpHttpResponseType.singleRecord);
		
		return response;
	}

	
	public List<CcpJsonRepresentation> getManyByIds(CcpEntityIdGenerator entity, String... ids) {
	
		SourceHandler mgetHandler = new SourceHandler(CcpConstants.EMPTY_JSON);
		List<CcpJsonRepresentation> asList = Arrays.asList(ids).stream()
				.map(id -> CcpConstants.EMPTY_JSON.put("id", id))
				.collect(Collectors.toList());
		List<CcpJsonRepresentation> asMapList = this.getManyByIds(mgetHandler, asList, entity);
		
		return asMapList;
	}

	
	public boolean exists(CcpEntityIdGenerator entity, String id) {
		String path = "/" + entity + "/_doc/" + id;
		
		CcpJsonRepresentation flows = CcpConstants.EMPTY_JSON.put("200", CcpHttpStatus.OK).put("404",  CcpHttpStatus.NOT_FOUND);
		
		CcpDbRequester dbUtils = CcpDependencyInjection.getDependency(CcpDbRequester.class);
		CcpJsonRepresentation response = dbUtils.executeHttpRequest("exists", path, "HEAD", flows, CcpConstants.EMPTY_JSON, CcpHttpResponseType.singleRecord);
		CcpHttpStatus status = response.getAsObject(CcpHttpStatus.class.getSimpleName());
		
		boolean exists = CcpHttpStatus.OK.equals(status);
		return exists;
	}

	
	public CcpJsonRepresentation createOrUpdate(CcpEntityIdGenerator entity, CcpJsonRepresentation data, String id) {
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

	private CcpJsonRepresentation retryCreateOrUpdate(CcpEntityIdGenerator entity, CcpJsonRepresentation data, String id) {
		new CcpTimeDecorator().sleep(1000);
		return this.createOrUpdate(entity, data, id);
	}

	
	public boolean delete(CcpEntityIdGenerator entity, String id) {
		String path = "/" + entity + "/_doc/" + id;
		CcpJsonRepresentation handlers = CcpConstants.EMPTY_JSON.put("200", CcpConstants.DO_NOTHING).put("404", CcpConstants.DO_NOTHING);
		CcpDbRequester dbUtils = CcpDependencyInjection.getDependency(CcpDbRequester.class);
		CcpJsonRepresentation response = dbUtils.executeHttpRequest("delete", path, "DELETE", handlers, CcpConstants.EMPTY_JSON, CcpHttpResponseType.singleRecord);
		String result = response.getAsString("result");
		boolean found = "deleted".equals( result);
		return found;
	}

	
	public CcpJsonRepresentation getAllData(CcpJsonRepresentation values, CcpEntityIdGenerator... entities) {
		
		SourceHandler mgetHandler = new SourceHandler(values);
		List<CcpJsonRepresentation> asList = Arrays.asList(values);
		List<CcpJsonRepresentation> results = this.getManyByIds(mgetHandler, asList, entities);
		
//		return asMapList;

		CcpJsonRepresentation response = this.extractResponse(results);

		return response;
	}

	private CcpJsonRepresentation extractResponse(List<CcpJsonRepresentation> results) {
		CcpJsonRepresentation response = CcpConstants.EMPTY_JSON;
		
		for (CcpJsonRepresentation result : results) {
			boolean notFound = result.getAsBoolean("_found") == false;
			if(notFound) {
				continue;
			}
			String entity = result.getAsString("_index");
			response = response.put(entity, result.removeKeys("_found", "_index"));
		}
		return response;
	}

	
	public List<CcpJsonRepresentation> getManyById(List<CcpJsonRepresentation> values, CcpEntityIdGenerator... entities) {
		
		SourceHandler mgetHandler = new SourceHandler(CcpConstants.EMPTY_JSON);
	
		List<CcpJsonRepresentation> asMapList = this.getManyByIds(mgetHandler, values, entities);
		
		List<CcpJsonRepresentation> collect = asMapList.stream().filter(x -> x.getAsBoolean("_found")).map(x -> x.removeKeys("_found", "_index")).collect(Collectors.toList());
		
		return collect;
	}


	public CcpDaoUnionAll unionAll(Collection<CcpJsonRepresentation> values, CcpEntityIdGenerator... entities) {
		SourceHandler mgetHandler = new SourceHandler(CcpConstants.EMPTY_JSON);
		List<CcpJsonRepresentation> asMapList = this.getManyByIds(mgetHandler, values, entities);
		CcpDaoUnionAll ccpDaoUnionAll = new CcpDaoUnionAll(asMapList);
		return ccpDaoUnionAll;
	}
	@Override
	public CcpDaoUnionAll unionAll(Set<String> values, CcpEntityIdGenerator... entities) {
		SourceHandler mgetHandler = new SourceHandler(CcpConstants.EMPTY_JSON);
		List<CcpJsonRepresentation> asMapList = this.getManyByIds(mgetHandler, values, entities);
		CcpDaoUnionAll ccpDaoUnionAll = new CcpDaoUnionAll(asMapList);
		return ccpDaoUnionAll;
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

