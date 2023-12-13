package eu.altfive.playground.repository;

import eu.altfive.playground.model.ElasticModel;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;

public interface ElasticModelRepository extends ElasticsearchRepository<ElasticModel, String> {

}
