package dev.vality.analytics.listener;

import dev.vality.analytics.AnalyticsApplication;
import dev.vality.analytics.dao.repository.postgres.party.management.CategoryDao;
import dev.vality.analytics.service.DominantService;
import dev.vality.analytics.utils.KafkaAbstractTest;
import dev.vality.analytics.utils.TestData;
import dev.vality.analytics.utils.Version;
import dev.vality.damsel.domain.CategoryType;
import dev.vality.damsel.domain.DomainObject;
import dev.vality.damsel.domain_config.Commit;
import dev.vality.damsel.domain_config.RepositorySrv;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.junit.ClassRule;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.testcontainers.containers.PostgreSQLContainer;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.when;

@Slf4j
@SpringBootTest(classes = AnalyticsApplication.class,
        properties = {"kafka.state.cache.size=0"})
@ContextConfiguration(initializers = {DominantListenerTest.Initializer.class})
public class DominantListenerTest extends KafkaAbstractTest {

    @ClassRule
    @SuppressWarnings("rawtypes")
    public static PostgreSQLContainer postgres = (PostgreSQLContainer) new PostgreSQLContainer(Version.POSTGRES_VERSION)
            .withStartupTimeout(Duration.ofMinutes(5));
    @Autowired
    private DominantService dominantService;
    @Autowired
    private CategoryDao categoryDao;
    @Autowired
    private JdbcTemplate postgresJdbcTemplate;
    @MockBean
    private RepositorySrv.Iface dominantClient;

    @BeforeEach
    public void setUp() throws Exception {
        postgresJdbcTemplate.execute("TRUNCATE TABLE analytics.category");
    }

    @Test
    public void testDominantProcessCommitsInsert() throws TException {
        Integer categoryId = 64;
        String categoryName = "testName";
        String categoryDescription = "testDescription";
        CategoryType categoryType = CategoryType.test;

        Map<Long, Commit> commits = new HashMap<>();
        commits.put(1L,
                TestData.buildInsertCategoryCommit(categoryId, categoryName, categoryDescription, categoryType));
        when(dominantClient.pullRange(anyLong(), anyInt())).thenReturn(commits);

        dominantService.pullDominantRange(10);

        dev.vality.analytics.domain.db.tables.pojos.Category category = categoryDao.getCategory(64, 1L);

        Assertions.assertEquals(categoryId, category.getCategoryId());
        Assertions.assertEquals(categoryName, category.getName());
        Assertions.assertEquals(categoryDescription, category.getDescription());
        Assertions.assertEquals(categoryType.name(), category.getType());
    }

    @Test
    public void testDominantProcessCommitsUpdate() throws TException {
        Integer categoryId = 64;
        String categoryName = "testName";
        String categoryDescription = "testDescription";
        CategoryType categoryType = CategoryType.test;
        String updatedCategoryName = "testNameNew";
        String updatedCategoryDescription = "testDescriptionNew";

        Map<Long, Commit> commits = new HashMap<>();
        Commit firstCommit =
                TestData.buildInsertCategoryCommit(categoryId, categoryName, categoryDescription, categoryType);
        DomainObject oldObject = firstCommit.getOps().get(0).getInsert().getObject();
        Commit secondCommit =
                TestData.buildUpdateCategoryCommit(categoryId, updatedCategoryName, updatedCategoryDescription,
                        categoryType, oldObject);
        commits.put(1L, firstCommit);
        commits.put(2L, secondCommit);
        when(dominantClient.pullRange(anyLong(), anyInt())).thenReturn(commits);

        dominantService.pullDominantRange(10);

        dev.vality.analytics.domain.db.tables.pojos.Category category = categoryDao.getCategory(64, 2L);

        Assertions.assertEquals(categoryId, category.getCategoryId());
        Assertions.assertEquals(updatedCategoryName, category.getName());
        Assertions.assertEquals(updatedCategoryDescription, category.getDescription());
    }

    @Test
    public void testDominantProcessCommitsRemove() throws TException {
        Integer categoryId = 64;
        String categoryName = "testName";
        String categoryDescription = "testDescription";
        CategoryType categoryType = CategoryType.test;
        Map<Long, Commit> commits = new HashMap<>();
        Commit firstCommit =
                TestData.buildInsertCategoryCommit(categoryId, categoryName, categoryDescription, categoryType);
        Commit secondCommit =
                TestData.buildRemoveCategoryCommit(categoryId, categoryName, categoryDescription, categoryType);
        commits.put(1L, firstCommit);
        commits.put(2L, secondCommit);

        when(dominantClient.pullRange(anyLong(), anyInt())).thenReturn(commits);

        dominantService.pullDominantRange(10);

        dev.vality.analytics.domain.db.tables.pojos.Category category = categoryDao.getCategory(64, 2L);

        Assertions.assertEquals(categoryId, category.getCategoryId());
        Assertions.assertEquals(categoryName, category.getName());
        Assertions.assertEquals(categoryDescription, category.getDescription());
        Assertions.assertEquals(categoryType.name(), category.getType());
        Assertions.assertTrue(category.getDeleted());
    }

    public static class Initializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {
        @Override
        public void initialize(ConfigurableApplicationContext configurableApplicationContext) {
            TestPropertyValues.of(
                    "postgres.db.url=" + postgres.getJdbcUrl(),
                    "postgres.db.user=" + postgres.getUsername(),
                    "postgres.db.password=" + postgres.getPassword(),
                    "spring.flyway.url=" + postgres.getJdbcUrl(),
                    "spring.flyway.user=" + postgres.getUsername(),
                    "spring.flyway.password=" + postgres.getPassword(),
                    "spring.flyway.enabled=true",
                    "service.dominant.scheduler.enabled=false")
                    .applyTo(configurableApplicationContext.getEnvironment());
            postgres.start();
        }
    }

}
