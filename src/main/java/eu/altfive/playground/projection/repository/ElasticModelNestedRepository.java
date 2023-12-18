package eu.altfive.playground.projection.repository;

import eu.altfive.playground.projection.model.ElasticModelNested;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;

public interface ElasticModelNestedRepository extends
    ElasticsearchRepository<ElasticModelNested, String> {

}
