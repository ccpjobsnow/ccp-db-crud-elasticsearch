package com.ccp.implementations.db.crud.elasticsearch;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.ccp.constantes.CcpConstants;
import com.ccp.decorators.CcpMapDecorator;
import com.ccp.dependency.injection.CcpDependencyInject;
import com.ccp.especifications.db.crud.CcpDbCrud;
import com.ccp.especifications.db.table.CcpDbTable;
import com.ccp.especifications.db.utils.CcpDbUtils;
import com.ccp.especifications.http.CcpHttpResponseType;
import com.ccp.exceptions.commons.CcpFlow;
import com.ccp.exceptions.db.CcpRecordNotFound;
import com.ccp.process.CcpProcess;


class DbCrudElasticSearch implements CcpDbCrud {

	@CcpDependencyInject
	private CcpDbUtils dbUtils;
	
	private final CcpSourceHandler mgetHandler = new CcpSourceHandler();

	@Override
	public List<CcpMapDecorator> getManyByIds(CcpMapDecorator filterEspecifications) {
		List<CcpMapDecorator> asList = filterEspecifications.keySet().stream().map(
				table -> new CcpMapDecorator()
				.put("_id", filterEspecifications.get(table))
				.put("_index", table))
				.collect(Collectors.toList());
		
		CcpMapDecorator requestBody = new CcpMapDecorator().put("docs", asList);
		
		List<CcpMapDecorator> collect = this.extractListFromMgetResponse(requestBody);
		
		return collect;
	}

	private List<CcpMapDecorator> extractListFromMgetResponse(CcpMapDecorator requestBody) {
		CcpMapDecorator response = this.dbUtils.executeHttpRequest("/_mget", "POST", 200, requestBody, CcpHttpResponseType.singleRecord);
		
		List<CcpMapDecorator> docs = response.getAsMapList("docs");
		List<CcpMapDecorator> collect = docs.stream().map(x -> this.mgetHandler.execute(x)).collect(Collectors.toList());
		return collect;
	}

	@Override
	public List<CcpMapDecorator> getManyById(CcpMapDecorator values, CcpDbTable... tables) {

		List<CcpMapDecorator> asList = Arrays.asList(tables).stream().map(
				table -> {
					String id = table.getId(values, table.getTimeOption(), table.getFields());
					return new CcpMapDecorator()
					.put("_id", id)
					.put("_index", table.name());
				})
				.collect(Collectors.toList());
		
		CcpMapDecorator requestBody = new CcpMapDecorator().put("docs", asList);
		
		List<CcpMapDecorator> asMapList = this.extractListFromMgetResponse(requestBody);
		
		return asMapList;
	}

	@Override
	public CcpMapDecorator getOneById(CcpDbTable tableName, String id) {
	
		String path = "/" + tableName + "/_doc/" + id + "/_source";
		
		CcpProcess throwNotFoundError =  x -> {
			throw new CcpRecordNotFound(tableName.name(), id);
		};

		CcpMapDecorator handlers = new CcpMapDecorator().put("200", CcpConstants.DO_NOTHING).put("404", throwNotFoundError);
		
		CcpMapDecorator response = this.dbUtils.executeHttpRequest(path, "GET", handlers, CcpConstants.EMPTY_JSON, CcpHttpResponseType.singleRecord);
		
		return response;
	}

	@Override
	public List<CcpMapDecorator> getManyByIds(CcpDbTable tableName, String... ids) {
	
		List<String> asList = Arrays.asList(ids);
		CcpMapDecorator requestBody = new CcpMapDecorator().put("ids", asList);
		List<CcpMapDecorator> asMapList = this.extractListFromMgetResponse(requestBody);
		return asMapList;
	}

	@Override
	public boolean exists(CcpDbTable tableName, String id) {
		String path = "/" + tableName + "/_doc/" + id;
		

		CcpMapDecorator handlers = new CcpMapDecorator().put("200", CcpHttpStatus.OK).put("404",  CcpHttpStatus.NOT_FOUND);
		
		CcpMapDecorator response = this.dbUtils.executeHttpRequest(path, "GET", handlers, CcpConstants.EMPTY_JSON, CcpHttpResponseType.singleRecord);
		CcpHttpStatus status = response.getAsObject(CcpHttpStatus.class.getSimpleName());
		
		boolean exists = CcpHttpStatus.OK.equals(status);
		return exists;
	}

	@Override
	public boolean updateOrSave(CcpMapDecorator data, CcpDbTable tableName, String id) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Set<String> getSynonyms(Set<String> wordsToAnalyze, CcpDbTable tableName, String... analyzers) {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public CcpMapDecorator findById(CcpMapDecorator values, CcpMapDecorator... roadMap) {
		CcpDbTable[]  tables = new CcpDbTable[roadMap.length];
		int k = 0;
		Set<CcpDbTable> keySet = Arrays.asList(roadMap).stream().map(x -> (CcpDbTable) x.getAsObject("table") ).collect(Collectors.toSet());
		for (CcpDbTable ccpDbTable : keySet) {
			tables[k++] = ccpDbTable;
		}

		List<CcpMapDecorator> manyById = this.getManyById(values, tables);
		
		k = 0;
		
		for (CcpMapDecorator dataBaseRow : manyById) {
		
			String tableName = dataBaseRow.getAsString("_index");

			boolean recordFound = dataBaseRow.getAsBoolean("found");
			
			Optional<CcpMapDecorator> findFirst = Arrays.asList(roadMap).stream()
					.filter(x -> x.getAsString("table").equals(tableName))
					.filter(x -> x.getAsBoolean("found") == recordFound)
					.findFirst();

			CcpMapDecorator record = dataBaseRow.getInternalMap("_source");
			
			boolean itWasNotForeseen = findFirst.isPresent() == false;
			
			if(itWasNotForeseen) {

				if(recordFound == false) {
					continue;
				}
				
				values = values.putSubKey("_tables", tableName, record);
				
				continue;
			}
			
			CcpMapDecorator specification = findFirst.get();
			
			boolean willNotExecuteAction = specification.containsKey("action") == false;
			
			if(willNotExecuteAction) {
			
				boolean willNotThrowException = specification.containsKey("status") == false;
				
				if(willNotThrowException) {
					continue;
				}
				
				Integer status = specification.getAsIntegerNumber("status");
				String message = specification.getAsString("message");
				throw new CcpFlow(values, status , message);
			}
			
			CcpProcess action = specification.getAsObject("action");

			if(recordFound == false) {
				CcpMapDecorator execute = action.execute(values);
				return execute;
			}
			
			CcpMapDecorator context = values.putSubKey("_tables", tableName, record);
			CcpMapDecorator execute = action.execute(context);
			return execute;
		}
		
		return values;
	}

	@Override
	public CcpMapDecorator remove(String id) {
		// TODO Auto-generated method stub
		return null;
		
	}

}
enum CcpHttpStatus implements CcpProcess{
	OK,
	NOT_FOUND;


	@Override
	public CcpMapDecorator execute(CcpMapDecorator values) {
		return values.put(CcpHttpStatus.class.getSimpleName(), this);
	}
	
}

