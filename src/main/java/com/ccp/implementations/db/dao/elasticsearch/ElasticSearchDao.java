package com.ccp.implementations.db.dao.elasticsearch;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.ccp.constantes.CcpConstants;
import com.ccp.decorators.CcpJsonRepresentation;
import com.ccp.decorators.CcpTimeDecorator;
import com.ccp.dependency.injection.CcpDependencyInjection;
import com.ccp.especifications.db.dao.CcpDao;
import com.ccp.especifications.db.utils.CcpDbRequester;
import com.ccp.especifications.db.utils.CcpEntityIdGenerator;
import com.ccp.especifications.http.CcpHttpResponseType;
import com.ccp.exceptions.db.CcpEntityRecordNotFound;
import com.ccp.exceptions.db.CcpEntityMissingKeys;
import com.ccp.exceptions.process.CcpThrowException;

class ElasticSearchDao implements CcpDao {


	private List<CcpJsonRepresentation> extractListFromMgetResponse(CcpJsonRepresentation requestBody) {
	
		CcpDbRequester dbUtils = CcpDependencyInjection.getDependency(CcpDbRequester.class);
		CcpJsonRepresentation response = dbUtils.executeHttpRequest("/_mget", "POST", 200, requestBody, CcpHttpResponseType.singleRecord);
		
		List<CcpJsonRepresentation> docs = response.getAsJsonList("docs");
		SourceHandler mgetHandler = new SourceHandler(requestBody);
		List<CcpJsonRepresentation> collect = docs.stream().map(x -> mgetHandler.apply(x)).collect(Collectors.toList());
		return collect;
	}

	
	public List<CcpJsonRepresentation> getManyById(CcpJsonRepresentation values, CcpEntityIdGenerator... entities) {

		List<CcpJsonRepresentation> asList = Arrays.asList(entities).stream().map(
				entity -> {
					String id = entity.getId(values);
					String entidade = entity.name();
					return CcpConstants.EMPTY_JSON
					.put("_id", id)
					.put("_index", entidade);
				})
				.collect(Collectors.toList());
		
		CcpJsonRepresentation requestBody = CcpConstants.EMPTY_JSON.put("docs", asList);
		
		List<CcpJsonRepresentation> asMapList = this.extractListFromMgetResponse(requestBody);
		
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
		CcpJsonRepresentation response = dbUtils.executeHttpRequest(path, "GET", handlers, CcpConstants.EMPTY_JSON, CcpHttpResponseType.singleRecord);
		
		return response;
	}

	
	public List<CcpJsonRepresentation> getManyByIds(CcpEntityIdGenerator entity, String... ids) {
	
		List<String> asList = Arrays.asList(ids);
		CcpJsonRepresentation requestBody = CcpConstants.EMPTY_JSON.put("ids", asList);
		List<CcpJsonRepresentation> asMapList = this.extractListFromMgetResponse(requestBody);
		return asMapList;
	}

	
	public boolean exists(CcpEntityIdGenerator entity, String id) {
		String path = "/" + entity + "/_doc/" + id;
		
		CcpJsonRepresentation flows = CcpConstants.EMPTY_JSON.put("200", CcpHttpStatus.OK).put("404",  CcpHttpStatus.NOT_FOUND);
		
		CcpDbRequester dbUtils = CcpDependencyInjection.getDependency(CcpDbRequester.class);
		CcpJsonRepresentation response = dbUtils.executeHttpRequest(path, "HEAD", flows, CcpConstants.EMPTY_JSON, CcpHttpResponseType.singleRecord);
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
		CcpJsonRepresentation response = dbUtils.executeHttpRequest(path, "POST", handlers, requestBody, CcpHttpResponseType.singleRecord);
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
		CcpJsonRepresentation response = dbUtils.executeHttpRequest(path, "DELETE", handlers, CcpConstants.EMPTY_JSON, CcpHttpResponseType.singleRecord);
		String result = response.getAsString("result");
		boolean found = "deleted".equals( result);
		return found;
	}

	
	public CcpJsonRepresentation getAllData(CcpJsonRepresentation values, CcpEntityIdGenerator... entities) {
		
		CcpJsonRepresentation requestBody = this.getRequestBody(values, entities);
		
		List<CcpJsonRepresentation> results = this.extractListFromMgetResponse(requestBody);

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

	private CcpJsonRepresentation getRequestBody(CcpJsonRepresentation values, CcpEntityIdGenerator... entities) {
		List<CcpJsonRepresentation> docs = new ArrayList<>();
		for (CcpEntityIdGenerator entity : entities) {
			try {
				String id = entity.getId(values);
				CcpJsonRepresentation doc = CcpConstants.EMPTY_JSON.put("_id", id).put("_index", entity.name());
				docs.add(doc);
			} catch (CcpEntityMissingKeys e) {
				continue;
			}
		}
		
		if(docs.isEmpty()) {
			return CcpConstants.EMPTY_JSON;
		}
		
		CcpJsonRepresentation requestBody = CcpConstants.EMPTY_JSON.put("docs", docs);
		return requestBody;
	}
	
	public List<CcpJsonRepresentation> getManyById(List<CcpJsonRepresentation> values, CcpEntityIdGenerator... entities) {
		List<CcpJsonRepresentation> docs = new ArrayList<CcpJsonRepresentation>();
		for (CcpEntityIdGenerator entity : entities) {
			String entidade = entity.name();
			for (CcpJsonRepresentation value : values) {
				String id = entity.getId(value);
				CcpJsonRepresentation put = CcpConstants.EMPTY_JSON
				.put("_id", id)
				.put("_index", entidade);
				docs.add(put);
			}
		}
		CcpJsonRepresentation requestBody = CcpConstants.EMPTY_JSON.put("docs", docs);
		
		List<CcpJsonRepresentation> asMapList = this.extractListFromMgetResponse(requestBody);
		List<CcpJsonRepresentation> collect = asMapList.stream().filter(x -> x.getAsBoolean("_found")).map(x -> x.removeKeys("_found", "_index")).collect(Collectors.toList());
		return collect;
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

