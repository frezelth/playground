package eu.altfive.playground.projection.repository;

import eu.altfive.playground.projection.model.ElasticModel;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;

public interface ElasticModelRepository extends ElasticsearchRepository<ElasticModel, String> {

}
