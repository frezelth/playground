package eu.altfive.playground.query;

import eu.altfive.playground.model.ElasticModel;
import eu.altfive.playground.query.SearchCriteria.SpecificAttributeCriteria;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.elasticsearch.client.elc.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.SearchHit;
import org.springframework.data.elasticsearch.core.SearchHits;
import org.springframework.data.elasticsearch.core.query.Criteria;
import org.springframework.data.elasticsearch.core.query.CriteriaQuery;
import org.springframework.data.elasticsearch.core.query.CriteriaQueryBuilder;
import org.springframework.data.elasticsearch.core.query.Query;
import org.springframework.data.elasticsearch.core.query.RuntimeField;
import org.springframework.stereotype.Service;

@Service
public class ElasticsearchQuery {

  private final ElasticsearchTemplate template;

  public ElasticsearchQuery(ElasticsearchTemplate template) {
    this.template = template;
  }

  public Page<SearchHit<ElasticModel>> search(SearchCriteria criteria, int limit, int offset){
    PageRequest pageRequest = PageRequest.of(offset / limit, limit);
    Criteria searchCriteria = new Criteria();
    List<RuntimeField> runtimeFields = new ArrayList<>();
    for (SpecificAttributeCriteria specificAttributeCriteria : criteria.specificAttributeCriteria()){
      searchCriteria = switch (specificAttributeCriteria.type()){
        case STRING -> searchCriteria
            .and(getSpecificAttributeCriteriaName(specificAttributeCriteria))
            .is(specificAttributeCriteria.stringValue());
        case BOOLEAN -> searchCriteria
            .and(getSpecificAttributeCriteriaName(specificAttributeCriteria))
            .is(specificAttributeCriteria.booleanValue());
        case INTEGER, LONG, DOUBLE -> {
          RuntimeField r = new RuntimeField("runtime_"+specificAttributeCriteria.name(), "long",
              "emit(Long.parseLong(doc['processVariables."+ElasticModel.LONG_PREFIX+specificAttributeCriteria.name()+"'].value));");
          runtimeFields.add(r);
//          yield searchCriteria;
          yield searchCriteria
              .and("runtime_"+specificAttributeCriteria.name())
              .between(
                  specificAttributeCriteria.numericValueGte().longValue(),
                  specificAttributeCriteria.numericValueLte().longValue()
              );
        }
        case DATE -> searchCriteria
            .and(getSpecificAttributeCriteriaName(specificAttributeCriteria))
            .between(
                Instant.ofEpochMilli(specificAttributeCriteria.dateValueGte()).toString(),
                Instant.ofEpochMilli(specificAttributeCriteria.dateValueLte()).toString()
                );
      };
    }
    CriteriaQueryBuilder builder = new CriteriaQueryBuilder(searchCriteria)
        .withPageable(pageRequest)
        .withFields(runtimeFields.stream().map(RuntimeField::getName).toList())
        .withRuntimeFields(runtimeFields);

    long before = System.currentTimeMillis();
    SearchHits<ElasticModel> search = template.search(builder.build(), ElasticModel.class);
    long after = System.currentTimeMillis();
    System.out.println("TIME:"+(after - before));
    return new PageImpl<>(search.getSearchHits(), pageRequest, search.getTotalHits());
  }

  private String getSpecificAttributeCriteriaName(SpecificAttributeCriteria criteria){
    String prefix = switch (criteria.type()){
      case STRING -> ElasticModel.STRING_PREFIX;
      case BOOLEAN -> ElasticModel.BOOLEAN_PREFIX;
      case LONG, INTEGER -> ElasticModel.LONG_PREFIX;
      case DOUBLE -> ElasticModel.DOUBLE_PREFIX;
      case DATE -> ElasticModel.DATE_PREFIX;
    };
    return "processVariables." + prefix + criteria.name();
  }

}
