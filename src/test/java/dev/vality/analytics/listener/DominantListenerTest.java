package dev.vality.analytics.listener;

import dev.vality.analytics.config.KafkaTest;
import dev.vality.analytics.config.PostgresqlTest;
import dev.vality.analytics.dao.repository.postgres.party.management.CategoryDao;
import dev.vality.analytics.utils.DominantSinkEventTestUtils;
import dev.vality.analytics.utils.TestData;
import dev.vality.damsel.domain.CategoryType;
import dev.vality.damsel.domain_config.RepositorySrv;
import dev.vality.machinegun.eventsink.SinkEvent;
import dev.vality.testcontainers.annotations.KafkaConfig;
import dev.vality.testcontainers.annotations.kafka.config.KafkaProducer;
import org.apache.thrift.TBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.util.List;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

@KafkaTest
@KafkaConfig
@SpringBootTest
@PostgresqlTest
public class DominantListenerTest {

    @Value("${kafka.topic.dominant.initial}")
    public String dominantTopic;
    @Autowired
    private KafkaProducer<TBase<?, ?>> testThriftKafkaProducer;
    @Autowired
    private CategoryDao categoryDao;
    @Autowired
    private JdbcTemplate postgresJdbcTemplate;

    @Test
    public void testDominantProcessCommitsInsert() {
        Integer categoryId = 64;
        String categoryName = "testName";
        String categoryDescription = "testDescription";
        CategoryType categoryType = CategoryType.test;


        var data = TestData.buildInsertCategoryOperation(categoryId, categoryName, categoryDescription, categoryType);
        final List<SinkEvent> sinkEvents = List.of(DominantSinkEventTestUtils.create(data, 1L));
        sinkEvents.forEach(event -> testThriftKafkaProducer.send(dominantTopic, event));

        await().atMost(60, SECONDS).until(() -> {
            Long version = postgresJdbcTemplate.query(
                    "SELECT version_id FROM analytics.category WHERE category_id = ?",
                    new Object[]{categoryId},
                    rs -> rs.next() ? rs.getLong("version_id") : null
            );
            return version != null && version == 1;
        });

        var category = categoryDao.getCategory(64, 1L);

        Assertions.assertEquals(categoryId, category.getCategoryId());
        Assertions.assertEquals(categoryName, category.getName());
        Assertions.assertEquals(categoryDescription, category.getDescription());
        Assertions.assertEquals(categoryType.name(), category.getType());
        Assertions.assertEquals(DominantSinkEventTestUtils.CHANGED_BY.getName(), category.getChangedByName());
        Assertions.assertEquals(DominantSinkEventTestUtils.CHANGED_BY.getEmail(), category.getChangedByEmail());
        Assertions.assertEquals(DominantSinkEventTestUtils.CHANGED_BY.getId(), category.getChangedById());
    }

    @Test
    public void testDominantProcessCommitsUpdate() {
        Integer categoryId = 64;
        String categoryName = "testName";
        String categoryDescription = "testDescription";
        CategoryType categoryType = CategoryType.test;
        String updatedCategoryName = "testNameNew";
        String updatedCategoryDescription = "testDescriptionNew";

        var data = TestData.buildInsertCategoryOperation(categoryId, categoryName, categoryDescription, categoryType);
        var updatedData = TestData.buildUpdateCategoryOperation(
                categoryId, updatedCategoryName, updatedCategoryDescription, categoryType
        );
        final List<SinkEvent> sinkEvents = List.of(
                DominantSinkEventTestUtils.create(data, 1L),
                DominantSinkEventTestUtils.create(updatedData, 2L)
        );

        sinkEvents.forEach(event -> testThriftKafkaProducer.send(dominantTopic, event));

        await().atMost(60, SECONDS).until(() -> {
            Long version = postgresJdbcTemplate.query(
                    "SELECT version_id FROM analytics.category WHERE category_id = ?",
                    new Object[]{categoryId},
                    rs -> rs.next() ? rs.getLong("version_id") : null
            );
            return version != null && version == 2;
        });

        var category = categoryDao.getCategory(64, 2L);

        Assertions.assertEquals(categoryId, category.getCategoryId());
        Assertions.assertEquals(updatedCategoryName, category.getName());
        Assertions.assertEquals(updatedCategoryDescription, category.getDescription());
        Assertions.assertEquals(DominantSinkEventTestUtils.CHANGED_BY.getName(), category.getChangedByName());
        Assertions.assertEquals(DominantSinkEventTestUtils.CHANGED_BY.getEmail(), category.getChangedByEmail());
        Assertions.assertEquals(DominantSinkEventTestUtils.CHANGED_BY.getId(), category.getChangedById());
    }

    @Test
    public void testDominantProcessCommitsRemove() {
        Integer categoryId = 64;
        String categoryName = "testName";
        String categoryDescription = "testDescription";
        CategoryType categoryType = CategoryType.test;

        var data = TestData.buildInsertCategoryOperation(categoryId, categoryName, categoryDescription, categoryType);
        var removedData = TestData.buildRemoveCategoryOperation(categoryId);
        final List<SinkEvent> sinkEvents = List.of(
                DominantSinkEventTestUtils.create(data, 1L),
                DominantSinkEventTestUtils.create(removedData, 2L)
        );

        sinkEvents.forEach(event -> testThriftKafkaProducer.send(dominantTopic, event));

        await().atMost(60, SECONDS).until(() -> {
            Long version = postgresJdbcTemplate.query(
                    "SELECT version_id FROM analytics.category WHERE category_id = ?",
                    new Object[]{categoryId},
                    rs -> rs.next() ? rs.getLong("version_id") : null
            );
            return version != null && version == 2;
        });

        var category = categoryDao.getCategory(64, 2L);

        Assertions.assertEquals(categoryId, category.getCategoryId());
        Assertions.assertEquals(categoryName, category.getName());
        Assertions.assertEquals(categoryDescription, category.getDescription());
        Assertions.assertEquals(categoryType.name(), category.getType());
        Assertions.assertTrue(category.getDeleted());
        Assertions.assertEquals(DominantSinkEventTestUtils.CHANGED_BY.getName(), category.getChangedByName());
        Assertions.assertEquals(DominantSinkEventTestUtils.CHANGED_BY.getEmail(), category.getChangedByEmail());
        Assertions.assertEquals(DominantSinkEventTestUtils.CHANGED_BY.getId(), category.getChangedById());
    }
}
