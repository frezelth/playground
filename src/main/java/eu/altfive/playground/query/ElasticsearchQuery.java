package eu.altfive.playground.query;

import eu.altfive.playground.model.ElasticModel;
import eu.altfive.playground.query.SearchCriteria.SpecificAttributeCriteria;
import java.time.Instant;
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
import org.springframework.stereotype.Service;

@Service
public class ElasticsearchQuery {

  private final ElasticsearchTemplate template;

  public ElasticsearchQuery(ElasticsearchTemplate template) {
    this.template = template;
  }

  public Page<SearchHit<ElasticModel>> search(SearchCriteria criteria, int limit, int offset){
    Criteria searchCriteria = new Criteria();

    for (SpecificAttributeCriteria specificAttributeCriteria : criteria.specificAttributeCriteria()){
      searchCriteria = switch (specificAttributeCriteria.type()){
        case STRING -> searchCriteria
            .and(getSpecificAttributeCriteriaName(specificAttributeCriteria))
            .is(specificAttributeCriteria.stringValue());
        case BOOLEAN -> searchCriteria
            .and(getSpecificAttributeCriteriaName(specificAttributeCriteria))
            .is(specificAttributeCriteria.booleanValue());
        case INTEGER, LONG, DOUBLE -> searchCriteria
            .and(getSpecificAttributeCriteriaName(specificAttributeCriteria))
            .is(specificAttributeCriteria.numericValueLte());
        case DATE -> searchCriteria
            .and(getSpecificAttributeCriteriaName(specificAttributeCriteria))
            .between(
                Instant.ofEpochMilli(specificAttributeCriteria.dateValueGte()).toString(),
                Instant.ofEpochMilli(specificAttributeCriteria.dateValueLte()).toString()
                );
      };
    }
    PageRequest pageRequest = PageRequest.of(offset / limit, limit);
    CriteriaQueryBuilder builder = new CriteriaQueryBuilder(searchCriteria)
        .withPageable(pageRequest);
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
      case INTEGER -> ElasticModel.INTEGER_PREFIX;
      case LONG -> ElasticModel.LONG_PREFIX;
      case DOUBLE -> ElasticModel.DOUBLE_PREFIX;
      case DATE -> ElasticModel.DATE_PREFIX;
    };
    return "processVariables." + prefix + criteria.name();
  }

}
